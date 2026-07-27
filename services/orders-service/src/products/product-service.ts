import { z } from "zod";
import { AppError } from "../errors";

interface DatabaseQuery {
  query<Row>(
    text: string,
    values?: readonly unknown[]
  ): Promise<{
    rows: Row[];
  }>;
}

export interface ProductCache {
  readonly isReady: boolean;

  get(key: string): Promise<string | null>;

  set(
    key: string,
    value: string,
    options: {
      EX: number;
    }
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
  updatedAt: z.string().datetime()
});

export type Product = z.infer<
  typeof cachedProductSchema
>;

export type ProductCacheStatus =
  | "HIT"
  | "MISS"
  | "BYPASS";

interface GetProductInput {
  tenantId: string;
  productId: string;
  cacheTtlSeconds: number;

  onCacheError?: (
    error: unknown,
    operation: "get" | "set" | "delete"
  ) => void;
}

interface GetProductResult {
  product: Product;
  cacheStatus: ProductCacheStatus;
}

export function createProductCacheKey(
  tenantId: string,
  productId: string
): string {
  return [
    "orderflow",
    "product",
    "v1",
    tenantId,
    productId
  ].join(":");
}

export async function getProductById(
  database: DatabaseQuery,
  cache: ProductCache,
  input: GetProductInput
): Promise<GetProductResult> {
  const cacheKey = createProductCacheKey(
    input.tenantId,
    input.productId
  );

  if (cache.isReady) {
    try {
      const cachedValue =
        await cache.get(cacheKey);

      if (cachedValue !== null) {
        const parsed =
          cachedProductSchema.safeParse(
            JSON.parse(cachedValue)
          );

        if (parsed.success) {
          return {
            product: parsed.data,
            cacheStatus: "HIT"
          };
        }

        /*
         * Invalid or obsolete cache data should not
         * be returned. Remove it and use PostgreSQL.
         */
        await cache.del(cacheKey);
      }
    } catch (error) {
      input.onCacheError?.(
        error,
        "get"
      );
    }
  }

  const result =
    await database.query<ProductRow>(
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
      [
        input.tenantId,
        input.productId
      ]
    );

  const row = result.rows[0];

  if (!row) {
    throw new AppError(
      404,
      "product_not_found",
      "Product was not found"
    );
  }

  const product: Product = {
    id: row.id,
    tenantId: row.tenant_id,
    sku: row.sku,
    name: row.name,
    priceMinor: row.price_minor,
    currency: row.currency,
    active: row.active,
    updatedAt:
      row.updated_at.toISOString()
  };

  if (!cache.isReady) {
    return {
      product,
      cacheStatus: "BYPASS"
    };
  }

  try {
    await cache.set(
      cacheKey,
      JSON.stringify(product),
      {
        EX: input.cacheTtlSeconds
      }
    );

    return {
      product,
      cacheStatus: "MISS"
    };
  } catch (error) {
    input.onCacheError?.(
      error,
      "set"
    );

    return {
      product,
      cacheStatus: "BYPASS"
    };
  }
}

export async function invalidateProductCache(
  cache: ProductCache,
  tenantId: string,
  productId: string
): Promise<void> {
  if (!cache.isReady) {
    return;
  }

  await cache.del(
    createProductCacheKey(
      tenantId,
      productId
    )
  );
}
