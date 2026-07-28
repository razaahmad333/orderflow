import pino from "pino";
import type { Pool } from "pg";

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { processOrderConfirmedNotification } from "../../src/notifications/order-confirmed-processor";

import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;

const logger = pino({
  level: "silent",
});

let orderId: string;

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);

  const result = await pool.query<{
    id: string;
  }>(
    `
      INSERT INTO orders (
        tenant_id,
        external_id,
        status,
        total_minor,
        currency
      )
      VALUES (
        $1,
        'notification-test-order',
        'confirmed',
        1299,
        'GBP'
      )
      RETURNING id
    `,
    [testTenantId],
  );

  orderId = result.rows[0]!.id;
});

afterAll(async () => {
  await pool.end();
});

describe("idempotent notification consumer", () => {
  it("does not deliver twice when processed twice", async () => {
    const data = {
      tenantId: testTenantId,
      orderId,
      externalId: "notification-test-order",
      totalMinor: "1299",
      currency: "GBP",
    };

    const first = await processOrderConfirmedNotification({
      pool,
      logger,
      jobId: "job-a",
      data,
      simulateCrashAfterProviderSuccess: false,
    });

    const second = await processOrderConfirmedNotification({
      pool,
      logger,
      jobId: "job-b",
      data,
      simulateCrashAfterProviderSuccess: false,
    });

    expect(first.providerCreatedNow).toBe(true);

    expect(second.skipped).toBe(true);

    const providerCount = await pool.query<{
      count: string;
    }>(
      `
          SELECT COUNT(*)
          FROM simulated_provider_messages
          WHERE order_id = $1
        `,
      [orderId],
    );

    expect(providerCount.rows[0]?.count).toBe("1");
  });

  it("recovers from a crash after provider success", async () => {
    const data = {
      tenantId: testTenantId,
      orderId,
      externalId: "notification-test-order",
      totalMinor: "1299",
      currency: "GBP",
    };

    await expect(
      processOrderConfirmedNotification({
        pool,
        logger,
        jobId: "crash-job",
        data,
        simulateCrashAfterProviderSuccess: true,
      }),
    ).rejects.toThrow("Simulated worker crash");

    const beforeRetry = await pool.query<{
      status: string;
    }>(
      `
          SELECT status
          FROM notification_deliveries
          WHERE order_id = $1
        `,
      [orderId],
    );

    expect(beforeRetry.rows[0]?.status).toBe("pending");

    const retry = await processOrderConfirmedNotification({
      pool,
      logger,
      jobId: "crash-job",
      data,
      simulateCrashAfterProviderSuccess: false,
    });

    /*
     * Provider already accepted the first request,
     * so the retry returns the original result.
     */
    expect(retry.providerCreatedNow).toBe(false);

    const providerCount = await pool.query<{
      count: string;
    }>(
      `
          SELECT COUNT(*)
          FROM simulated_provider_messages
          WHERE order_id = $1
        `,
      [orderId],
    );

    expect(providerCount.rows[0]?.count).toBe("1");

    const delivery = await pool.query<{
      status: string;
      attempt_count: number;
    }>(
      `
          SELECT
            status,
            attempt_count
          FROM notification_deliveries
          WHERE order_id = $1
        `,
      [orderId],
    );

    expect(delivery.rows[0]).toEqual({
      status: "delivered",
      attempt_count: 2,
    });
  });
});
