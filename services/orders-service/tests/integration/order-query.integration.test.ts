import type { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { listHighValueOrders } from "../../src/orders/order-query-service";

import {
  createTestPool,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);
});

afterAll(async () => {
  await pool.end();
});

describe("high-value order query", () => {
  it("returns orders by descending total", async () => {
    await pool.query(
      `
        INSERT INTO orders (
          tenant_id,
          external_id,
          status,
          total_minor,
          currency
        )
        VALUES
          ($1, 'low-order',  'confirmed', 1000, 'GBP'),
          ($1, 'top-order',  'confirmed', 9000, 'GBP'),
          ($1, 'mid-order',  'confirmed', 4000, 'GBP'),
          ($1, 'cancelled',  'cancelled', 20000, 'GBP')
      `,
      [testTenantId],
    );

    const result = await listHighValueOrders(pool, {
      tenantId: testTenantId,
      status: "confirmed",
      limit: 2,
    });

    expect(result.map((order) => order.externalId)).toEqual([
      "top-order",
      "mid-order",
    ]);

    expect(result.map((order) => order.totalMinor)).toEqual(["9000", "4000"]);
  });
});
