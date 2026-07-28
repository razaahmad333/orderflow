import { z } from "zod";

const environmentSchema = z.object({
  NODE_ENV: z
    .enum(["development", "test", "production"])
    .default("development"),

  SERVICE_NAME: z.string().trim().min(1, "SERVICE_NAME is required"),

  PORT: z.coerce.number().int().positive().max(65_535).default(3000),

  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"])
    .default("info"),

  SHUTDOWN_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .positive()
    .max(60_000)
    .default(10_000),

  DATABASE_URL: z.string().trim().min(1, "DATABASE_URL is required"),

  DB_POOL_MAX: z.coerce.number().int().min(1).max(50).default(10),

  DB_CONNECT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(30_000)
    .default(2_000),

  DB_HEALTHCHECK_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(500)
    .max(60_000)
    .default(2_000),

  ORDER_TRANSACTION_HOLD_MS: z.coerce
    .number()
    .int()
    .min(0)
    .max(10_000)
    .default(0),

  TRANSACTION_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(5).default(3),

  TRANSACTION_RETRY_BASE_DELAY_MS: z.coerce
    .number()
    .int()
    .min(10)
    .max(1000)
    .default(50),

  TRANSACTION_LOCK_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(50)
    .max(30_000)
    .default(1_000),

  TRANSACTION_STATEMENT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(60_000)
    .default(5_000),

  REDIS_URL: z.string().url().default("redis://localhost:6380"),

  REDIS_CONNECT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(10_000)
    .default(1_000),

  PRODUCT_CACHE_TTL_SECONDS: z.coerce
    .number()
    .int()
    .min(1)
    .max(86_400)
    .default(60),

  PRODUCT_CACHE_LOCK_TTL_MS: z.coerce
    .number()
    .int()
    .min(500)
    .max(30_000)
    .default(3_000),

  PRODUCT_CACHE_LOCK_WAIT_MS: z.coerce
    .number()
    .int()
    .min(0)
    .max(10_000)
    .default(1_000),

  PRODUCT_CACHE_LOCK_POLL_MS: z.coerce
    .number()
    .int()
    .min(10)
    .max(1_000)
    .default(50),

  CACHE_INVALIDATION_POLL_MS: z.coerce
    .number()
    .int()
    .min(250)
    .max(60_000)
    .default(2_000),

  CACHE_INVALIDATION_BATCH_SIZE: z.coerce
    .number()
    .int()
    .min(1)
    .max(100)
    .default(20),

  CACHE_INVALIDATION_LOCK_MS: z.coerce
    .number()
    .int()
    .min(1_000)
    .max(300_000)
    .default(30_000),
  BACKGROUND_JOB_POLL_MS: z.coerce
    .number()
    .int()
    .min(250)
    .max(60_000)
    .default(1_000),

  BACKGROUND_JOB_BATCH_SIZE: z.coerce
    .number()
    .int()
    .min(1)
    .max(100)
    .default(20),

  BACKGROUND_JOB_LOCK_MS: z.coerce
    .number()
    .int()
    .min(1_000)
    .max(300_000)
    .default(30_000),

  NOTIFICATION_JOB_ATTEMPTS: z.coerce.number().int().min(1).max(20).default(3),

  NOTIFICATION_JOB_BACKOFF_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(60_000)
    .default(1_000),

  NOTIFICATION_WORKER_CONCURRENCY: z.coerce
    .number()
    .int()
    .min(1)
    .max(100)
    .default(5),

  SIMULATE_NOTIFICATION_FAILURES: z.coerce
    .number()
    .int()
    .min(0)
    .max(20)
    .default(0),

  SIMULATE_NOTIFICATION_CRASH_AFTER_SEND: z.coerce
    .number()
    .int()
    .min(0)
    .max(1)
    .default(0),

  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) =>
      value
        .split(",")
        .map((broker) => broker.trim())
        .filter(Boolean),
    ),

  KAFKA_CLIENT_ID: z.string().min(1).default("orders-service"),

  KAFKA_ORDER_CREATED_TOPIC: z.string().min(1).default("order.created.v1"),
});

export type AppConfig = z.infer<typeof environmentSchema>;

export function loadConfig(
  environment: NodeJS.ProcessEnv = process.env,
): AppConfig {
  const result = environmentSchema.safeParse(environment);

  if (!result.success) {
    const issues = result.error.issues.map((issue) => ({
      field: issue.path.join("."),
      message: issue.message,
    }));

    throw new Error(
      `Invalid environment configuration: ${JSON.stringify(issues)}`,
    );
  }

  return result.data;
}
