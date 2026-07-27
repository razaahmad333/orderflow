import { Router } from "express";
import type { Pool } from "pg";
import { z } from "zod";

import { AppError } from "../errors";

import {
  getProductById,
  type ProductCache
} from "./product-service";

import type { SingleFlight } from "../single-flight";

const productParamsSchema = z.object({
  productId: z.string().uuid()
});

const productQuerySchema = z.object({
  tenantId: z.string().uuid()
});

interface CreateProductRouterOptions {
  cacheTtlSeconds: number;
}

export function createProductRouter(
  pool: Pool,
  cache: ProductCache,
  coordinator: SingleFlight,
  options: CreateProductRouterOptions,
): Router {
  const router = Router();

  router.get(
    "/:productId",

    async (request, response, next) => {
      try {
        const parsedParams = productParamsSchema.safeParse(request.params);

        const parsedQuery = productQuerySchema.safeParse(request.query);

        if (!parsedParams.success || !parsedQuery.success) {
          throw new AppError(
            400,
            "invalid_request",
            "Product request validation failed",
            {
              params: parsedParams.success
                ? undefined
                : parsedParams.error.flatten(),

              query: parsedQuery.success
                ? undefined
                : parsedQuery.error.flatten(),
            },
          );
        }

        const result = await getProductById(pool, cache, {
          tenantId: parsedQuery.data.tenantId,

          productId: parsedParams.data.productId,

          cacheTtlSeconds: options.cacheTtlSeconds,
          coordinator,
          onCacheError(error, operation) {
            request.log.warn(
              {
                err: error,
                cacheOperation: operation,
              },
              "Product cache operation failed",
            );
          },
        });

        response.setHeader("x-cache", result.cacheStatus);

        response.status(200).json(result.product);
      } catch (error) {
        next(error);
      }
    },
  );

  return router;
}
