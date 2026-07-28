import type { Pool } from "pg";

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { placeOrder } from "../../src/orders/order-service";

import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;

const options = {
  maxTransactionAttempts: 3,
  transactionRetryBaseDelayMs: 10,
  transactionLockTimeoutMs: 2000,
  transactionStatementTimeoutMs: 5000,
  orderCreatedTopic: "order.created.v1",
};

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);

  await pool.query("TRUNCATE kafka_outbox");
});

afterAll(async () => {
  await pool.end();
});

describe("order Kafka outbox", () => {
  it("commits the order and event together", async () => {
    const order = await placeOrder(
      pool,
      {
        tenantId: testTenantId,
        externalId: "outbox-order-001",
        currency: "GBP",

        items: [
          {
            productId: keyboardProductId,
            quantity: 1,
          },
        ],
      },
      options,
    );

    expect(order.created).toBe(true);

    const outbox = await pool.query<{
      aggregate_id: string;
      event_type: string;
      topic: string;
      payload: {
        externalId: string;
      };
    }>(
      `
          SELECT
            aggregate_id,
            event_type,
            topic,
            payload
          FROM kafka_outbox
        `,
    );

    expect(outbox.rows).toHaveLength(1);

    expect(outbox.rows[0]).toMatchObject({
      aggregate_id: order.id,
      event_type: "order.created",
      topic: "order.created.v1",

      payload: {
        externalId: "outbox-order-001",
      },
    });
  });

  it("rolls back the event when order placement fails", async () => {
    await expect(
      placeOrder(
        pool,
        {
          tenantId: testTenantId,
          externalId: "outbox-failed-order",
          currency: "GBP",

          items: [
            {
              productId: keyboardProductId,
              quantity: 1000,
            },
          ],
        },
        options,
      ),
    ).rejects.toMatchObject({
      code: "insufficient_inventory",
    });

    const result = await pool.query<{
      count: string;
    }>(
      `
          SELECT COUNT(*)
          FROM kafka_outbox
        `,
    );

    expect(result.rows[0]?.count).toBe("0");
  });
});
