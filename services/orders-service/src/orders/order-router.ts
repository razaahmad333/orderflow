import { Router } from "express";
import type { Pool } from "pg";

import { AppError } from "../errors";
import { placeOrderSchema } from "./order-schema";
import { placeOrder } from "./order-service";
 
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
