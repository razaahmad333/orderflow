import { z } from "zod";
import { AppError } from "../errors";
import type { DatabaseQuery } from "../database-query";
import type { SingleFlight } from "../single-flight";

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

export type ProductCacheStatus = "HIT" | "MISS" | "COALESCED" | "BYPASS";

interface GetProductInput {
  tenantId: string;
  productId: string;
  cacheTtlSeconds: number;

  coordinator?: SingleFlight;

  onCacheError?: (error: unknown, operation: "get" | "set" | "delete") => void;
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

export async function getProductById(
  database: DatabaseQuery,
  cache: ProductCache,
  input: GetProductInput,
): Promise<GetProductResult> {
  const cacheKey = createProductCacheKey(input.tenantId, input.productId);

  /*
   * Fast path: return immediately when Redis contains
   * a valid cached product.
   */
  if (cache.isReady) {
    try {
      const cachedValue = await cache.get(cacheKey);

      if (cachedValue !== null) {
        const parsed = cachedProductSchema.safeParse(JSON.parse(cachedValue));

        if (parsed.success) {
          return {
            product: parsed.data,
            cacheStatus: "HIT",
          };
        }

        await cache.del(cacheKey);
      }
    } catch (error) {
      input.onCacheError?.(error, "get");
    }
  }

  async function loadAndCache(): Promise<{
    product: Product;
    cacheWriteSucceeded: boolean;
  }> {
    /*
     * A request may have populated Redis after our
     * first GET but before this caller became leader.
     *
     * Rechecking avoids an unnecessary PostgreSQL query
     * in that race.
     */
    if (cache.isReady) {
      try {
        const cachedValue = await cache.get(cacheKey);

        if (cachedValue !== null) {
          const parsed = cachedProductSchema.safeParse(JSON.parse(cachedValue));

          if (parsed.success) {
            return {
              product: parsed.data,
              cacheWriteSucceeded: true,
            };
          }

          await cache.del(cacheKey);
        }
      } catch (error) {
        input.onCacheError?.(error, "get");
      }
    }

    const product = await loadProductFromDatabase(
      database,
      input.tenantId,
      input.productId,
    );

    if (!cache.isReady) {
      return {
        product,
        cacheWriteSucceeded: false,
      };
    }

    try {
      await cache.set(cacheKey, JSON.stringify(product), {
        EX: input.cacheTtlSeconds,
      });

      return {
        product,
        cacheWriteSucceeded: true,
      };
    } catch (error) {
      input.onCacheError?.(error, "set");

      return {
        product,
        cacheWriteSucceeded: false,
      };
    }
  }

  if (!input.coordinator) {
    const result = await loadAndCache();

    return {
      product: result.product,

      cacheStatus: result.cacheWriteSucceeded ? "MISS" : "BYPASS",
    };
  }

  const coordinated = await input.coordinator.run(cacheKey, loadAndCache);

  if (coordinated.shared) {
    return {
      product: coordinated.value.product,
      cacheStatus: "COALESCED",
    };
  }

  return {
    product: coordinated.value.product,

    cacheStatus: coordinated.value.cacheWriteSucceeded ? "MISS" : "BYPASS",
  };
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
