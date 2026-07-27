import { Router } from "express";
import type { Pool } from "pg";
import { z } from "zod";

import { AppError } from "../errors";

import {
  getProductById,
  type ProductCache
} from "./product-service";

import type { SingleFlight } from "../single-flight";
import type { DistributedLock } from "../distributed-lock";
import {
  updateProductBodySchema,
  updateProductParamsSchema,
} from "./product-update-schema";

import { updateProduct } from "./product-service";

const productParamsSchema = z.object({
  productId: z.string().uuid()
});

const productQuerySchema = z.object({
  tenantId: z.string().uuid()
});

interface CreateProductRouterOptions {
  cacheTtlSeconds: number;
  lockTtlMs: number;
  lockWaitMs: number;
  lockPollMs: number;

  maxTransactionAttempts: number;
  transactionRetryBaseDelayMs: number;
  transactionLockTimeoutMs: number;
  transactionStatementTimeoutMs: number;
}

export function createProductRouter(
  pool: Pool,
  cache: ProductCache,
  coordinator: SingleFlight,
  distributedLock: DistributedLock,
  options: CreateProductRouterOptions,
): Router {
  const router = Router();

  router.patch(
    "/:productId",

    async (request, response, next) => {
      try {
        const parsedParams = updateProductParamsSchema.safeParse(
          request.params,
        );

        const parsedBody = updateProductBodySchema.safeParse(request.body);

        if (!parsedParams.success || !parsedBody.success) {
          throw new AppError(
            400,
            "invalid_request",
            "Product update validation failed",
            {
              params: parsedParams.success
                ? undefined
                : parsedParams.error.flatten(),

              body: parsedBody.success ? undefined : parsedBody.error.flatten(),
            },
          );
        }

        const result = await updateProduct(
          pool,
          cache,
          {
            ...parsedBody.data,

            productId: parsedParams.data.productId,
          },
          {
            maxTransactionAttempts: options.maxTransactionAttempts,

            transactionRetryBaseDelayMs: options.transactionRetryBaseDelayMs,

            transactionLockTimeoutMs: options.transactionLockTimeoutMs,

            transactionStatementTimeoutMs:
              options.transactionStatementTimeoutMs,

            onTransactionRetry(event) {
              request.log.warn(
                {
                  ...event,
                  tenantId: parsedBody.data.tenantId,
                  productId: parsedParams.data.productId,
                },
                "Retrying product update transaction",
              );
            },

            onCacheError(error, operation) {
              request.log.warn(
                {
                  err: error,
                  cacheOperation: operation,
                  productId: parsedParams.data.productId,
                },
                "Product cache invalidation failed",
              );
            },
          },
        );

        response.setHeader("x-cache-invalidation", result.cacheInvalidation);

        response.status(200).json(result.product);
      } catch (error) {
        next(error);
      }
    },
  );
  
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

          distributedLock,

          lockTtlMs: options.lockTtlMs,

          lockWaitMs: options.lockWaitMs,

          lockPollMs: options.lockPollMs,
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
