import pino from "pino";
import request from "supertest";
import { beforeEach, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { createApp, type ReadinessState } from "../src/app";
import type { AppConfig } from "../src/config";
import type { ProductCache } from "../src/products/product-service";
import { SingleFlight } from "../src/single-flight";

const config: AppConfig = {
  NODE_ENV: "test",
  SERVICE_NAME: "orders-service",
  PORT: 3000,
  LOG_LEVEL: "silent",
  SHUTDOWN_TIMEOUT_MS: 1000,
  DATABASE_URL: "postgresql://test:test@localhost:5432/test",
  DB_POOL_MAX: 5,
  DB_CONNECT_TIMEOUT_MS: 1000,
  DB_HEALTHCHECK_INTERVAL_MS: 1000,
  ORDER_TRANSACTION_HOLD_MS: 0,
  TRANSACTION_MAX_ATTEMPTS: 3,
  TRANSACTION_RETRY_BASE_DELAY_MS: 50,
  TRANSACTION_LOCK_TIMEOUT_MS: 1000,
  TRANSACTION_STATEMENT_TIMEOUT_MS: 5000,
  REDIS_URL: "redis://localhost:6380",
  REDIS_CONNECT_TIMEOUT_MS: 1000,
  PRODUCT_CACHE_TTL_SECONDS: 60,
  PRODUCT_CACHE_LOCK_TTL_MS: 3000,
  PRODUCT_CACHE_LOCK_WAIT_MS: 1000,
  PRODUCT_CACHE_LOCK_POLL_MS: 50,
  CACHE_INVALIDATION_POLL_MS: 2000,
  CACHE_INVALIDATION_BATCH_SIZE: 20,
  CACHE_INVALIDATION_LOCK_MS: 30000,

  BACKGROUND_JOB_POLL_MS: 1000,
  BACKGROUND_JOB_BATCH_SIZE: 20,
  BACKGROUND_JOB_LOCK_MS: 30000,

  NOTIFICATION_JOB_ATTEMPTS: 3,
  NOTIFICATION_JOB_BACKOFF_MS: 1000,
  NOTIFICATION_WORKER_CONCURRENCY: 5,

  SIMULATE_NOTIFICATION_FAILURES: 0
};

const logger = pino({
  level: "silent",
});
const pool = {} as Pool;

const redis = {
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
} satisfies ProductCache;

describe("orders-service", () => {
  let readiness: ReadinessState;
  let app: ReturnType<typeof createApp>;
  const productSingleFlight = new SingleFlight();
  const productDistributedLock = {
    async acquire() {
      return null;
    },
  };
  beforeEach(() => {
    readiness = {
      ready: true,
    };

    app = createApp({
      config,
      logger,
      readiness,
      pool,
      redis,
      productSingleFlight,
      productDistributedLock,
    });
  });

  it("returns a successful liveness response", async () => {
    const response = await request(app).get("/health/live").expect(200);

    expect(response.body.status).toBe("alive");
    expect(response.body.service).toBe("orders-service");
    expect(response.body.uptimeSeconds).toBeTypeOf("number");
  });

  it("returns ready while the service can receive traffic", async () => {
    const response = await request(app).get("/health/ready").expect(200);

    expect(response.body).toEqual({
      status: "ready",
      service: "orders-service",
    });
  });

  it("returns 503 when the service is not ready", async () => {
    readiness.ready = false;
    readiness.reason = "dependency_unavailable";

    const response = await request(app).get("/health/ready").expect(503);

    expect(response.body).toEqual({
      status: "not-ready",
      service: "orders-service",
      reason: "dependency_unavailable",
    });
  });

  it("generates a request ID", async () => {
    const response = await request(app).get("/health/live").expect(200);
    const requestId = response.headers["x-request-id"];

    expect(requestId).toBeTypeOf("string");
    expect(requestId).toBeDefined();
    expect(requestId!.length).toBeGreaterThan(0);
  });

  it("preserves an incoming request ID", async () => {
    await request(app)
      .get("/health/live")
      .set("x-request-id", "orderflow-test-request")
      .expect("x-request-id", "orderflow-test-request")
      .expect(200);
  });
});
