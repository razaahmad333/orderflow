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

import type { ProductCache } from "../../src/products/product-service";

import {
  createProductCacheKey,
  updateProduct,
} from "../../src/products/product-service";

import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;

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
});

afterAll(async () => {
  await pool.end();
});

describe("product update integration", () => {
  it("updates the product and invalidates cache after commit", async () => {
    const del = vi.fn().mockResolvedValue(1);

    const cache: ProductCache = {
      isReady: true,

      async get() {
        return null;
      },

      async set() {
        return "OK";
      },

      del,
    };

    const result = await updateProduct(
      pool,
      cache,
      {
        tenantId: testTenantId,
        productId: keyboardProductId,
        expectedVersion: "0",
        name: "Keyboard Pro",
        priceMinor: "1599",
      },
      transactionOptions,
    );

    expect(result.product).toMatchObject({
      name: "Keyboard Pro",
      priceMinor: "1599",
      version: "1",
    });

    expect(result.cacheInvalidation).toBe("DELETED");

    expect(del).toHaveBeenCalledWith(
      createProductCacheKey(testTenantId, keyboardProductId),
    );

    const databaseResult = await pool.query<{
      name: string;
      price_minor: string;
      version: string;
    }>(
      `
          SELECT
            name,
            price_minor,
            version
          FROM products
          WHERE tenant_id = $1
            AND id = $2
        `,
      [testTenantId, keyboardProductId],
    );

    expect(databaseResult.rows[0]).toEqual({
      name: "Keyboard Pro",
      price_minor: "1599",
      version: "1",
    });
  });

  it("rejects two concurrent updates using the same version", async () => {
    const cache: ProductCache = {
      isReady: false,

      async get() {
        return null;
      },

      async set() {
        return undefined;
      },

      async del() {
        return 0;
      },
    };

    const results = await Promise.allSettled([
      updateProduct(
        pool,
        cache,
        {
          tenantId: testTenantId,
          productId: keyboardProductId,
          expectedVersion: "0",
          name: "Update A",
        },
        transactionOptions,
      ),

      updateProduct(
        pool,
        cache,
        {
          tenantId: testTenantId,
          productId: keyboardProductId,
          expectedVersion: "0",
          name: "Update B",
        },
        transactionOptions,
      ),
    ]);

    const fulfilled = results.filter((result) => result.status === "fulfilled");

    const rejected = results.filter((result) => result.status === "rejected");

    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);

    expect((rejected[0] as PromiseRejectedResult).reason).toMatchObject({
      code: "product_version_conflict",
    });

    const product = await pool.query<{
      name: string;
      version: string;
    }>(
      `
          SELECT name, version
          FROM products
          WHERE tenant_id = $1
            AND id = $2
        `,
      [testTenantId, keyboardProductId],
    );

    expect(product.rows[0]?.version).toBe("1");

    expect(["Update A", "Update B"]).toContain(product.rows[0]?.name);
  });
});
