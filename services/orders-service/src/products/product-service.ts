import { z } from "zod";
import { AppError } from "../errors";
import type { DatabaseQuery } from "../database-query";
import type { SingleFlight } from "../single-flight";
import type { DistributedLock } from "../distributed-lock";
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
}

const cachedProductSchema = z.object({
  id: z.string().uuid(),
  tenantId: z.string().uuid(),
  sku: z.string(),
  name: z.string(),
  priceMinor: z.string(),
  currency: z.string(),
  active: z.boolean(),
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
          updated_at
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
