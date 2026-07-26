import type { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { placeOrder } from "../../src/orders/order-service";

import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  mouseProductId,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;

const transactionOptions = {
  maxTransactionAttempts: 3,
  transactionRetryBaseDelayMs: 10,
  transactionLockTimeoutMs: 2_000,
  transactionStatementTimeoutMs: 5_000,
};

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);
});

afterAll(async () => {
  await pool?.end();
});

describe("order placement integration", () => {
  it("commits an order and inventory changes atomically", async () => {
    const order = await placeOrder(
      pool,
      {
        tenantId: testTenantId,
        externalId: "integration-order-001",
        currency: "GBP",

        items: [
          {
            productId: keyboardProductId,
            quantity: 2,
          },
          {
            productId: mouseProductId,
            quantity: 1,
          },
        ],
      },
      transactionOptions,
    );

    expect(order).toMatchObject({
      externalId: "integration-order-001",
      status: "confirmed",
      totalMinor: "5197",
      created: true,
    });

    const inventory = await pool.query<{
      product_id: string;
      available_quantity: number;
      version: string;
    }>(
      `
          SELECT
            product_id,
            available_quantity,
            version
          FROM inventory
          ORDER BY product_id
        `,
    );

    expect(inventory.rows).toEqual([
      {
        product_id: keyboardProductId,
        available_quantity: 8,
        version: "1",
      },
      {
        product_id: mouseProductId,
        available_quantity: 4,
        version: "1",
      },
    ]);
  });

  it("rolls back the pending order when inventory is insufficient", async () => {
    await expect(
      placeOrder(
        pool,
        {
          tenantId: testTenantId,
          externalId: "integration-insufficient-001",
          currency: "GBP",

          items: [
            {
              productId: keyboardProductId,
              quantity: 100,
            },
          ],
        },
        transactionOptions,
      ),
    ).rejects.toMatchObject({
      code: "insufficient_inventory",
    });

    const orderCount = await pool.query<{
      count: string;
    }>(
      `
          SELECT COUNT(*)
          FROM orders
          WHERE external_id =
            'integration-insufficient-001'
        `,
    );

    expect(orderCount.rows[0]?.count).toBe("0");

    const inventory = await pool.query<{
      available_quantity: number;
      version: string;
    }>(
      `
          SELECT
            available_quantity,
            version
          FROM inventory
          WHERE product_id = $1
        `,
      [keyboardProductId],
    );

    expect(inventory.rows[0]).toEqual({
      available_quantity: 10,
      version: "0",
    });
  });

  it("returns the existing result for an identical replay", async () => {
    const input = {
      tenantId: testTenantId,
      externalId: "integration-replay-001",
      currency: "GBP" as const,

      items: [
        {
          productId: keyboardProductId,
          quantity: 2,
        },
      ],
    };

    const first = await placeOrder(pool, input, transactionOptions);

    const replay = await placeOrder(pool, input, transactionOptions);

    expect(first.created).toBe(true);
    expect(replay.created).toBe(false);
    expect(replay.id).toBe(first.id);

    const inventory = await pool.query<{
      available_quantity: number;
    }>(
      `
          SELECT available_quantity
          FROM inventory
          WHERE product_id = $1
        `,
      [keyboardProductId],
    );

    expect(inventory.rows[0]?.available_quantity).toBe(8);
  });

  it("prevents concurrent orders from overselling inventory", async () => {
    const results = await Promise.allSettled([
      placeOrder(
        pool,
        {
          tenantId: testTenantId,
          externalId: "concurrent-integration-a",
          currency: "GBP",

          items: [
            {
              productId: keyboardProductId,
              quantity: 7,
            },
          ],
        },
        {
          ...transactionOptions,
          transactionHoldMs: 100,
        },
      ),

      placeOrder(
        pool,
        {
          tenantId: testTenantId,
          externalId: "concurrent-integration-b",
          currency: "GBP",

          items: [
            {
              productId: keyboardProductId,
              quantity: 7,
            },
          ],
        },
        {
          ...transactionOptions,
          transactionHoldMs: 100,
        },
      ),
    ]);

    const fulfilled = results.filter((result) => result.status === "fulfilled");

    const rejected = results.filter((result) => result.status === "rejected");

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);

    expect((rejected[0] as PromiseRejectedResult).reason).toMatchObject({
      code: "insufficient_inventory",
    });

    const inventory = await pool.query<{
      available_quantity: number;
      version: string;
    }>(
      `
          SELECT
            available_quantity,
            version
          FROM inventory
          WHERE product_id = $1
        `,
      [keyboardProductId],
    );

    expect(inventory.rows[0]).toEqual({
      available_quantity: 3,
      version: "1",
    });

    const orders = await pool.query<{
      count: string;
    }>(
      `
          SELECT COUNT(*)
          FROM orders
        `,
    );

    expect(orders.rows[0]?.count).toBe("1");
  });
});
