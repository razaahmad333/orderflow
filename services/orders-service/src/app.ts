import crypto from "node:crypto";

import express, {
  type NextFunction,
  type Request,
  type Response,
} from "express";
import type { Pool } from "pg";
import type { Logger } from "pino";
import pinoHttp from "pino-http";

import type { AppConfig } from "./config";
import { AppError } from "./errors";
import { createOrderRouter } from "./orders/order-router";
import { createProductRouter } from "./products/product-router";

import type { ProductCache } from "./products/product-service";

import type { SingleFlight } from "./single-flight";
import type { DistributedLock } from "./distributed-lock";
import type { OrderEventPublisher } from "./events/order-event-publisher";
export interface ReadinessState {
  ready: boolean;
  reason?: string;
}
interface CreateAppDependencies {
  config: AppConfig;
  logger: Logger;
  readiness: ReadinessState;
  pool: Pool;
  redis: ProductCache;
  productSingleFlight: SingleFlight;
  productDistributedLock: DistributedLock;
  orderEventPublisher: OrderEventPublisher;
}


export function createApp({
  config,
  logger,
  readiness,
  pool,
  redis,
  productSingleFlight,
  productDistributedLock,
  orderEventPublisher,
}: CreateAppDependencies) {
  const app = express();

  app.disable("x-powered-by");

  app.use(
    pinoHttp({
      logger,

      genReqId(request, response) {
        const suppliedRequestId = request.headers["x-request-id"];

        const requestId =
          typeof suppliedRequestId === "string" &&
          suppliedRequestId.trim().length > 0
            ? suppliedRequestId
            : crypto.randomUUID();

        response.setHeader("x-request-id", requestId);

        return requestId;
      },
    }),
  );

  app.use(
    express.json({
      limit: "1mb",
    }),
  );

  app.get("/health/live", (_request, response) => {
    response.status(200).json({
      status: "alive",
      service: config.SERVICE_NAME,
      uptimeSeconds: Math.floor(process.uptime()),
    });
  });

  app.get("/health/ready", (_request, response) => {
    if (!readiness.ready) {
      response.status(503).json({
        status: "not-ready",
        service: config.SERVICE_NAME,
        reason: readiness.reason ?? "unknown",
      });

      return;
    }

    response.status(200).json({
      status: "ready",
      service: config.SERVICE_NAME,
    });
  });

  app.use(
    "/orders",
    createOrderRouter(pool, orderEventPublisher,{
      transactionHoldMs: config.ORDER_TRANSACTION_HOLD_MS,

      maxTransactionAttempts: config.TRANSACTION_MAX_ATTEMPTS,

      transactionRetryBaseDelayMs: config.TRANSACTION_RETRY_BASE_DELAY_MS,

      transactionLockTimeoutMs: config.TRANSACTION_LOCK_TIMEOUT_MS,

      transactionStatementTimeoutMs: config.TRANSACTION_STATEMENT_TIMEOUT_MS,
    }),
  );

  app.use(
    "/products",
    createProductRouter(
      pool,
      redis,
      productSingleFlight,
      productDistributedLock,
      {
        cacheTtlSeconds: config.PRODUCT_CACHE_TTL_SECONDS,

        lockTtlMs: config.PRODUCT_CACHE_LOCK_TTL_MS,

        lockWaitMs: config.PRODUCT_CACHE_LOCK_WAIT_MS,

        lockPollMs: config.PRODUCT_CACHE_LOCK_POLL_MS,

        maxTransactionAttempts: config.TRANSACTION_MAX_ATTEMPTS,

        transactionRetryBaseDelayMs: config.TRANSACTION_RETRY_BASE_DELAY_MS,

        transactionLockTimeoutMs: config.TRANSACTION_LOCK_TIMEOUT_MS,

        transactionStatementTimeoutMs: config.TRANSACTION_STATEMENT_TIMEOUT_MS,
      },
    ),
  );

  app.use((request, response) => {
    response.status(404).json({
      error: "route_not_found",
      path: request.path,
      requestId: request.id,
    });
  });

  app.use(
    (
      error: unknown,
      request: Request,
      response: Response,
      _next: NextFunction,
    ) => {
      if (error instanceof AppError) {
        request.log.warn(
          {
            errorCode: error.code,
            details: error.details,
          },
          error.message,
        );

        response.status(error.statusCode).json({
          error: error.code,
          message: error.message,
          details: error.details,
          requestId: request.id,
        });

        return;
      }

      request.log.error(
        {
          err: error,
        },
        "Unhandled request error",
      );

      response.status(500).json({
        error: "internal_server_error",
        requestId: request.id,
      });
    },
  );

  return app;
}
