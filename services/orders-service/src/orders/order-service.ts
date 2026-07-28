import { createHash } from "node:crypto";

import type { Pool, PoolClient } from "pg";

import { AppError } from "../errors";
import type { PlaceOrderInput } from "./order-schema";
import {
  type TransactionRetryEvent,
  withTransactionRetry,
} from "../transaction";
import { enqueueBackgroundJob } from "../jobs/background-job-outbox";

import { notificationQueueName } from "../jobs/bullmq";
interface ProductInventoryRow {
  product_id: string;
  price_minor: string;
  currency: string;
  active: boolean;
  available_quantity: number;
}
interface PlaceOrderOptions {
  transactionHoldMs?: number;

  maxTransactionAttempts?: number;
  transactionRetryBaseDelayMs?: number;

  transactionLockTimeoutMs?: number;
  transactionStatementTimeoutMs?: number;

  onTransactionRetry?: (event: TransactionRetryEvent) => void;
}

interface OrderRow {
  id: string;
  tenant_id: string;
  external_id: string;
  status: string;
  total_minor: string;
  currency: string;
  request_fingerprint: string | null;
}

export interface PlaceOrderResult {
  id: string;
  tenantId: string;
  externalId: string;
  status: string;
  totalMinor: string;
  currency: string;
  created: boolean;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function createRequestFingerprint(input: PlaceOrderInput): string {
  const canonicalRequest = {
    tenantId: input.tenantId,
    externalId: input.externalId,
    currency: input.currency,
    items: [...input.items].sort((left, right) =>
      left.productId.localeCompare(right.productId),
    ),
  };

  return createHash("sha256")
    .update(JSON.stringify(canonicalRequest))
    .digest("hex");
}

function mapOrder(order: OrderRow, created: boolean): PlaceOrderResult {
  return {
    id: order.id,
    tenantId: order.tenant_id,
    externalId: order.external_id,
    status: order.status,
    totalMinor: order.total_minor,
    currency: order.currency,
    created,
  };
}

async function findExistingOrder(
  client: PoolClient,
  tenantId: string,
  externalId: string,
): Promise<OrderRow> {
  const result = await client.query<OrderRow>(
    `
      SELECT
        id,
        tenant_id,
        external_id,
        status,
        total_minor,
        currency,
        request_fingerprint
      FROM orders
      WHERE tenant_id = $1
        AND external_id = $2
    `,
    [tenantId, externalId],
  );

  const order = result.rows[0];

  if (!order) {
    throw new Error("Order conflict occurred but existing order was not found");
  }

  return order;
}
export async function placeOrder(
  pool: Pool,
  input: PlaceOrderInput,
  options: PlaceOrderOptions = {},
): Promise<PlaceOrderResult> {
  const fingerprint = createRequestFingerprint(input);

  return withTransactionRetry(
    pool,

    async (client) => {
      const insertedOrder = await client.query<OrderRow>(
        `
            INSERT INTO orders (
              tenant_id,
              external_id,
              status,
              total_minor,
              currency,
              request_fingerprint
            )
            VALUES (
              $1,
              $2,
              'pending',
              0,
              $3,
              $4
            )
            ON CONFLICT (
              tenant_id,
              external_id
            )
            DO NOTHING
            RETURNING
              id,
              tenant_id,
              external_id,
              status,
              total_minor,
              currency,
              request_fingerprint
          `,
        [input.tenantId, input.externalId, input.currency, fingerprint],
      );

      if (insertedOrder.rowCount === 0) {
        const existingOrder = await findExistingOrder(
          client,
          input.tenantId,
          input.externalId,
        );

        if (
          existingOrder.request_fingerprint !== null &&
          existingOrder.request_fingerprint !== fingerprint
        ) {
          throw new AppError(
            409,
            "idempotency_conflict",
            "The external order ID was already used with different order data",
          );
        }

        return mapOrder(existingOrder, false);
      }

      const order = insertedOrder.rows[0];

      if (!order) {
        throw new Error("Order insert succeeded without returning an order");
      }

      const productIds = input.items.map((item) => item.productId);

      const productResult = await client.query<ProductInventoryRow>(
        `
            SELECT
              p.id AS product_id,
              p.price_minor,
              p.currency,
              p.active,
              i.available_quantity
            FROM products p
            JOIN inventory i
              ON i.product_id = p.id
             AND i.tenant_id = p.tenant_id
            WHERE p.tenant_id = $1
              AND p.id = ANY($2::uuid[])
            ORDER BY p.id
            FOR UPDATE OF i
          `,
        [input.tenantId, productIds],
      );

      const transactionHoldMs = options.transactionHoldMs ?? 0;

      if (transactionHoldMs > 0) {
        await delay(transactionHoldMs);
      }

      if (productResult.rowCount !== input.items.length) {
        throw new AppError(
          404,
          "product_not_found",
          "One or more products do not exist for this tenant",
        );
      }

      const productsById = new Map(
        productResult.rows.map((product) => [product.product_id, product]),
      );

      let orderTotal = 0n;

      for (const item of input.items) {
        const product = productsById.get(item.productId);

        if (!product) {
          throw new AppError(
            404,
            "product_not_found",
            `Product ${item.productId} was not found`,
          );
        }

        if (!product.active) {
          throw new AppError(
            409,
            "product_inactive",
            `Product ${item.productId} is inactive`,
          );
        }

        if (product.currency !== input.currency) {
          throw new AppError(
            409,
            "currency_mismatch",
            `Product ${item.productId} uses ${product.currency}`,
          );
        }

        if (product.available_quantity < item.quantity) {
          throw new AppError(
            409,
            "insufficient_inventory",
            `Insufficient inventory for product ${item.productId}`,
            {
              productId: item.productId,
              requested: item.quantity,
              available: product.available_quantity,
            },
          );
        }

        const unitPrice = BigInt(product.price_minor);

        orderTotal += unitPrice * BigInt(item.quantity);

        await client.query(
          `
            INSERT INTO order_items (
              order_id,
              product_id,
              tenant_id,
              quantity,
              unit_price_minor
            )
            VALUES ($1, $2, $3, $4, $5)
          `,
          [
            order.id,
            item.productId,
            input.tenantId,
            item.quantity,
            unitPrice.toString(),
          ],
        );

        await client.query(
          `
            UPDATE inventory
            SET
              available_quantity =
                available_quantity - $1,
              version = version + 1,
              updated_at = NOW()
            WHERE product_id = $2
              AND tenant_id = $3
          `,
          [item.quantity, item.productId, input.tenantId],
        );
      }

      const confirmedOrder = await client.query<OrderRow>(
        `
            UPDATE orders
            SET
              status = 'confirmed',
              total_minor = $1,
              updated_at = NOW()
            WHERE id = $2
            RETURNING
              id,
              tenant_id,
              external_id,
              status,
              total_minor,
              currency,
              request_fingerprint
          `,
        [orderTotal.toString(), order.id],
      );

      const confirmed = confirmedOrder.rows[0];

      if (!confirmed) {
        throw new Error("Confirmed order was not returned");
      }

      await enqueueBackgroundJob(client, {
        tenantId: input.tenantId,
        queueName: notificationQueueName,
        jobType: "order-confirmed",

        payload: {
          tenantId: input.tenantId,
          orderId: confirmed.id,
          externalId: confirmed.external_id,
          totalMinor: confirmed.total_minor,
          currency: confirmed.currency,
        },
      });

      return mapOrder(confirmed, true);
    },

    {
      maxAttempts: options.maxTransactionAttempts,

      baseDelayMs: options.transactionRetryBaseDelayMs,

      onRetry: options.onTransactionRetry,

      lockTimeoutMs: options.transactionLockTimeoutMs,

      statementTimeoutMs: options.transactionStatementTimeoutMs,
    },
  );
}