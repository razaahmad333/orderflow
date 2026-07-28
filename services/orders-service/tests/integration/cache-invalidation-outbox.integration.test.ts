import type { Pool } from "pg";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

import { runCacheInvalidationBatch } from "../../src/cache-invalidation-worker";

import {
  updateProduct,
  type ProductCache,
} from "../../src/products/product-service";

import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

import pino from "pino";

let pool: Pool;

const logger = pino({
  level: "silent",
});

const transactionOptions = {
  maxTransactionAttempts: 3,
  transactionRetryBaseDelayMs: 10,
  transactionLockTimeoutMs: 2000,
  transactionStatementTimeoutMs: 5000,
};

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);

  await pool.query("TRUNCATE cache_invalidation_outbox");
});

afterAll(async () => {
  await pool.end();
});

describe("cache invalidation outbox", () => {
  it("retries a failed invalidation later", async () => {
    const failingCache: ProductCache = {
      isReady: true,

      async get() {
        return null;
      },

      async set() {
        return "OK";
      },

      async del() {
        throw new Error("Redis unavailable");
      },
    };

    const result = await updateProduct(
      pool,
      failingCache,
      {
        tenantId: testTenantId,
        productId: keyboardProductId,
        expectedVersion: "0",
        name: "Durable Keyboard",
      },
      transactionOptions,
    );

    expect(result.cacheInvalidation).toBe("QUEUED");

    const pending = await pool.query<{
      status: string;
      processed_at: Date | null;
    }>(
      `
          SELECT
            status,
            processed_at
          FROM cache_invalidation_outbox
        `,
    );

    expect(pending.rows[0]).toEqual({
      status: "pending",
      processed_at: null,
    });

    const del = vi.fn().mockResolvedValue(1);

    const healthyCache: ProductCache = {
      isReady: true,

      async get() {
        return null;
      },

      async set() {
        return "OK";
      },

      del,
    };

    const processed = await runCacheInvalidationBatch({
      pool,
      cache: healthyCache,
      logger,
      workerId: "integration-worker",
      batchSize: 10,
      lockMs: 30_000,
    });

    expect(processed).toBe(1);
    expect(del).toHaveBeenCalledTimes(1);

    const completed = await pool.query<{
      status: string;
      processed_at: Date | null;
    }>(
      `
          SELECT
            status,
            processed_at
          FROM cache_invalidation_outbox
        `,
    );

    expect(completed.rows[0]?.status).toBe("processed");

    expect(completed.rows[0]?.processed_at).toBeInstanceOf(Date);
  });
});
