import { z } from "zod";
import { AppError } from "../errors";
import type { DatabaseQuery } from "../database-query";
import type { SingleFlight } from "../single-flight";
import type { DistributedLock } from "../distributed-lock";
import type { TransactionRetryEvent } from "../transaction";
import { withTransactionRetry } from "../transaction";
import type { UpdateProductInput } from "./product-update-schema";
import {
  enqueueCacheInvalidation,
  markCacheInvalidationProcessed,
} from "../cache-invalidation-outbox";
export interface ProductCache {
  readonly isReady: boolean;

  get(key: string): Promise<string | null>;

  set(
    key: string,
    value: string,
    options: {
      EX: number;
    },
  ): Promise<unknown>;

  del(key: string): Promise<number>;
}

interface ProductRow {
  id: string;
  tenant_id: string;
  sku: string;
  name: string;
  price_minor: string;
  currency: string;
  active: boolean;
  updated_at: Date;
  version: string;
}

const cachedProductSchema = z.object({
  id: z.string().uuid(),
  tenantId: z.string().uuid(),
  sku: z.string(),
  name: z.string(),
  priceMinor: z.string(),
  currency: z.string(),
  active: z.boolean(),
  version: z.string().regex(/^\d+$/),
  updatedAt: z.string().datetime(),
});

export type Product = z.infer<typeof cachedProductSchema>;

export type ProductCacheStatus =
  | "HIT"
  | "MISS"
  | "COALESCED"
  | "LOCK_WAIT_HIT"
  | "BYPASS";

interface GetProductInput {
  tenantId: string;
  productId: string;
  cacheTtlSeconds: number;

  coordinator?: SingleFlight;
  distributedLock?: DistributedLock;

  lockTtlMs?: number;
  lockWaitMs?: number;
  lockPollMs?: number;

  onCacheError?: (
    error: unknown,
    operation: "get" | "set" | "delete" | "lock" | "unlock",
  ) => void;
}
interface GetProductResult {
  product: Product;
  cacheStatus: ProductCacheStatus;
}

interface UpdateProductOptions {
  maxTransactionAttempts: number;
  transactionRetryBaseDelayMs: number;
  transactionLockTimeoutMs: number;
  transactionStatementTimeoutMs: number;

  onTransactionRetry?: (event: TransactionRetryEvent) => void;

  onCacheError?: (error: unknown, operation: "delete") => void;
}

export type CacheInvalidationStatus = "DELETED" | "QUEUED";

export interface UpdateProductResult {
  product: Product;
  cacheInvalidation: CacheInvalidationStatus;
}

export function createProductCacheKey(
  tenantId: string,
  productId: string,
): string {
  return ["orderflow", "product", "v1", tenantId, productId].join(":");
}

async function loadProductFromDatabase(
  database: DatabaseQuery,
  tenantId: string,
  productId: string,
): Promise<Product> {
  const result = await database.query<ProductRow>(
    `
        SELECT
          id,
          tenant_id,
          sku,
          name,
          price_minor,
          currency,
          active,
          updated_at,
          version
        FROM products
        WHERE tenant_id = $1
          AND id = $2
      `,
    [tenantId, productId],
  );

  const row = result.rows[0];

  if (!row) {
    throw new AppError(404, "product_not_found", "Product was not found");
  }

  return {
    id: row.id,
    tenantId: row.tenant_id,
    sku: row.sku,
    name: row.name,
    priceMinor: row.price_minor,
    currency: row.currency,
    active: row.active,
    version: row.version,
    updatedAt: row.updated_at.toISOString(),
  };
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function readCachedProduct(
  cache: ProductCache,
  cacheKey: string,
  onCacheError: GetProductInput["onCacheError"] | undefined,
): Promise<Product | null> {
  if (!cache.isReady) {
    return null;
  }

  try {
    const cachedValue = await cache.get(cacheKey);

    if (cachedValue === null) {
      return null;
    }

    const parsed = cachedProductSchema.safeParse(JSON.parse(cachedValue));

    if (parsed.success) {
      return parsed.data;
    }

    await cache.del(cacheKey);

    return null;
  } catch (error) {
    onCacheError?.(error, "get");
    return null;
  }
}

async function writeCachedProduct(
  cache: ProductCache,
  cacheKey: string,
  product: Product,
  ttlSeconds: number,
  onCacheError: GetProductInput["onCacheError"] | undefined,
): Promise<boolean> {
  if (!cache.isReady) {
    return false;
  }

  try {
    await cache.set(cacheKey, JSON.stringify(product), {
      EX: ttlSeconds,
    });

    return true;
  } catch (error) {
    onCacheError?.(error, "set");
    return false;
  }
}

async function waitForCachedProduct(
  cache: ProductCache,
  cacheKey: string,
  waitMs: number,
  pollMs: number,
  onCacheError: GetProductInput["onCacheError"] | undefined,
): Promise<Product | null> {
  const deadline = Date.now() + waitMs;

  while (Date.now() < deadline) {
    await delay(pollMs);

    const product = await readCachedProduct(cache, cacheKey, onCacheError);

    if (product) {
      return product;
    }
  }

  return null;
}

export async function getProductById(
  database: DatabaseQuery,
  cache: ProductCache,
  input: GetProductInput,
): Promise<GetProductResult> {
  const cacheKey = createProductCacheKey(input.tenantId, input.productId);

  const initialCacheValue = await readCachedProduct(
    cache,
    cacheKey,
    input.onCacheError,
  );

  if (initialCacheValue) {
    return {
      product: initialCacheValue,
      cacheStatus: "HIT",
    };
  }

  async function loadAuthoritativeProduct(): Promise<{
    product: Product;
    cacheStatus: ProductCacheStatus;
  }> {
    /*
     * Another local or remote request may have filled
     * Redis since this request's first cache lookup.
     */
    const secondCacheValue = await readCachedProduct(
      cache,
      cacheKey,
      input.onCacheError,
    );

    if (secondCacheValue) {
      return {
        product: secondCacheValue,
        cacheStatus: "HIT",
      };
    }

    const distributedLock = input.distributedLock;

    if (distributedLock && cache.isReady) {
      let lease;

      try {
        lease = await distributedLock.acquire(
          cacheKey,
          input.lockTtlMs ?? 3_000,
        );
      } catch (error) {
        input.onCacheError?.(error, "lock");
      }

      if (lease) {
        try {
          /*
           * Recheck after acquiring the lease. A previous
           * owner may have populated Redis just before
           * releasing the lock.
           */
          const afterLockCacheValue = await readCachedProduct(
            cache,
            cacheKey,
            input.onCacheError,
          );

          if (afterLockCacheValue) {
            return {
              product: afterLockCacheValue,
              cacheStatus: "HIT",
            };
          }

          const product = await loadProductFromDatabase(
            database,
            input.tenantId,
            input.productId,
          );

          const cached = await writeCachedProduct(
            cache,
            cacheKey,
            product,
            input.cacheTtlSeconds,
            input.onCacheError,
          );

          return {
            product,
            cacheStatus: cached ? "MISS" : "BYPASS",
          };
        } finally {
          try {
            await lease.release();
          } catch (error) {
            input.onCacheError?.(error, "unlock");
          }
        }
      }

      const remotelyLoadedProduct = await waitForCachedProduct(
        cache,
        cacheKey,
        input.lockWaitMs ?? 1_000,
        input.lockPollMs ?? 50,
        input.onCacheError,
      );

      if (remotelyLoadedProduct) {
        return {
          product: remotelyLoadedProduct,
          cacheStatus: "LOCK_WAIT_HIT",
        };
      }

      /*
       * Bounded wait expired. Fall back to PostgreSQL
       * rather than making the endpoint unavailable.
       */
    }

    const product = await loadProductFromDatabase(
      database,
      input.tenantId,
      input.productId,
    );

    const cached = await writeCachedProduct(
      cache,
      cacheKey,
      product,
      input.cacheTtlSeconds,
      input.onCacheError,
    );

    return {
      product,
      cacheStatus: cached ? "MISS" : "BYPASS",
    };
  }

  if (!input.coordinator) {
    return loadAuthoritativeProduct();
  }

  const coordinated = await input.coordinator.run(
    cacheKey,
    loadAuthoritativeProduct,
  );

  if (coordinated.shared) {
    return {
      product: coordinated.value.product,
      cacheStatus: "COALESCED",
    };
  }

  return coordinated.value;
}

export async function invalidateProductCache(
  cache: ProductCache,
  tenantId: string,
  productId: string,
): Promise<void> {
  if (!cache.isReady) {
    return;
  }

  await cache.del(createProductCacheKey(tenantId, productId));
}

export async function updateProduct(
  pool: import("pg").Pool,
  cache: ProductCache,
  input: UpdateProductInput,
  options: UpdateProductOptions,
): Promise<UpdateProductResult> {
  const transactionResult = await withTransactionRetry(
    pool,

    async (client) => {
      const result = await client.query<ProductRow>(
        `
            UPDATE products
            SET
              name = COALESCE(
                $4,
                name
              ),

              price_minor = COALESCE(
                $5::BIGINT,
                price_minor
              ),

              active = COALESCE(
                $6,
                active
              ),

              version = version + 1,
              updated_at = NOW()

            WHERE tenant_id = $1
              AND id = $2
              AND version = $3::BIGINT

            RETURNING
              id,
              tenant_id,
              sku,
              name,
              price_minor,
              currency,
              active,
              version,
              updated_at
          `,
        [
          input.tenantId,
          input.productId,
          input.expectedVersion,
          input.name ?? null,
          input.priceMinor ?? null,
          input.active ?? null,
        ],
      );

      const updated = result.rows[0];

      if (updated) {
  
        const product: Product = {
          id: updated.id,
          tenantId: updated.tenant_id,
          sku: updated.sku,
          name: updated.name,
          priceMinor: updated.price_minor,
          currency: updated.currency,
          active: updated.active,
          version: updated.version,
          updatedAt: updated.updated_at.toISOString(),
        };

        const cacheKey = createProductCacheKey(input.tenantId, input.productId);

        const invalidationEventId = await enqueueCacheInvalidation(client, {
          tenantId: input.tenantId,
          entityType: "product",
          entityId: input.productId,
          cacheKey,
        });

        return {
          product,
          cacheKey,
          invalidationEventId,
        };
      }

      const current = await client.query<{
        version: string;
      }>(
        `
            SELECT version
            FROM products
            WHERE tenant_id = $1
              AND id = $2
          `,
        [input.tenantId, input.productId],
      );

      const existing = current.rows[0];

      if (!existing) {
        throw new AppError(404, "product_not_found", "Product was not found");
      }

      throw new AppError(
        409,
        "product_version_conflict",
        "The product was modified by another request",
        {
          expectedVersion: input.expectedVersion,

          currentVersion: existing.version,
        },
      );
    },

    {
      maxAttempts: options.maxTransactionAttempts,

      baseDelayMs: options.transactionRetryBaseDelayMs,

      lockTimeoutMs: options.transactionLockTimeoutMs,

      statementTimeoutMs: options.transactionStatementTimeoutMs,

      onRetry: options.onTransactionRetry,
    },
  );

  /*
   * withTransactionRetry returns only after COMMIT.
   * Cache invalidation therefore happens after the
   * authoritative database change becomes visible.
   */
  if (!cache.isReady) {
    return {
      product: transactionResult.product,

      cacheInvalidation: "QUEUED",
    };
  }

  try {
    await cache.del(transactionResult.cacheKey);

    /*
     * If this database update fails, the worker may
     * repeat the Redis deletion. DEL is idempotent,
     * so duplicate processing is harmless.
     */
    await markCacheInvalidationProcessed(
      pool,
      transactionResult.invalidationEventId,
    );

    return {
      product: transactionResult.product,

      cacheInvalidation: "DELETED",
    };
  } catch (error) {
    options.onCacheError?.(error, "delete");

    return {
      product: transactionResult.product,

      cacheInvalidation: "QUEUED",
    };
  }
}
