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
