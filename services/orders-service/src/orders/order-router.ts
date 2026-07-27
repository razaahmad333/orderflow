import { Router } from "express";
import type { Pool } from "pg";

import { AppError } from "../errors";
import { placeOrderSchema } from "./order-schema";
import { placeOrder } from "./order-service";
import { listOrdersQuerySchema } from "./order-list-schema";

import { highValueOrdersQuerySchema } from "./high-value-order-schema";

import { listHighValueOrders, listOrders } from "./order-query-service";
interface CreateOrderRouterOptions {
  transactionHoldMs: number;

  maxTransactionAttempts: number;
  transactionRetryBaseDelayMs: number;

  transactionLockTimeoutMs: number;
  transactionStatementTimeoutMs: number;
}

export function createOrderRouter(
  pool: Pool,
  options: CreateOrderRouterOptions,
): Router {
  const router = Router();

  router.get(
    "/high-value",

    async (request, response, next) => {
      try {
        const parsedQuery = highValueOrdersQuerySchema.safeParse(request.query);

        if (!parsedQuery.success) {
          throw new AppError(
            400,
            "invalid_query",
            "High-value order query validation failed",
            parsedQuery.error.flatten(),
          );
        }

        const orders = await listHighValueOrders(pool, parsedQuery.data);

        response.status(200).json({
          items: orders,
        });
      } catch (error) {
        next(error);
      }
    },
  );

  router.get("/", async (request, response, next) => {
    try {
      const parsedQuery = listOrdersQuerySchema.safeParse(request.query);

      if (!parsedQuery.success) {
        throw new AppError(
          400,
          "invalid_query",
          "Order query validation failed",
          parsedQuery.error.flatten(),
        );
      }

      const page = await listOrders(pool, parsedQuery.data);

      response.status(200).json(page);
    } catch (error) {
      next(error);
    }
  });

  router.post("/", async (request, response, next) => {
    try {
      const parsedRequest = placeOrderSchema.safeParse(request.body);

      if (!parsedRequest.success) {
        throw new AppError(
          400,
          "invalid_request",
          "Order request validation failed",
          parsedRequest.error.flatten(),
        );
      }

      const order = await placeOrder(pool, parsedRequest.data, {
        transactionHoldMs: options.transactionHoldMs,

        maxTransactionAttempts: options.maxTransactionAttempts,

        transactionRetryBaseDelayMs: options.transactionRetryBaseDelayMs,
        transactionLockTimeoutMs: options.transactionLockTimeoutMs,

        transactionStatementTimeoutMs: options.transactionStatementTimeoutMs,
        onTransactionRetry(event) {
          request.log.warn(
            {
              ...event,
              tenantId: parsedRequest.data.tenantId,
              externalId: parsedRequest.data.externalId,
            },
            "Retrying order transaction",
          );
        },
      });

      response.status(order.created ? 201 : 200).json(order);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
