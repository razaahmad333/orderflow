import { Pool } from "pg";
import type { Logger } from "pino";

import type { AppConfig } from "./config";

export function createDatabasePool(
  config: AppConfig,
  logger: Logger
): Pool {
  const pool = new Pool({
    connectionString: config.DATABASE_URL,
    max: config.DB_POOL_MAX,
    connectionTimeoutMillis: config.DB_CONNECT_TIMEOUT_MS,
    idleTimeoutMillis: 30_000,
    application_name: config.SERVICE_NAME
  });

  pool.on("connect", () => {
    logger.debug("PostgreSQL connection established");
  });

  pool.on("error", (error) => {
    logger.error(
      { err: error },
      "Unexpected error from an idle PostgreSQL client"
    );
  });

  return pool;
}
