import "dotenv/config";

import pino from "pino";
import { Pool } from "pg";

import { withTransactionRetry } from "../transaction";

const productId = "10000000-0000-4000-8000-000000000001";

interface PostgreSqlError {
  code?: string;
  message?: string;
}

const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: {
    lab: "lock-timeout",
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const pool = new Pool({
    connectionString: databaseUrl,
    max: 3,
    application_name: "lock-timeout-lab",
  });

  const lockHolder = await pool.connect();

  try {
    await lockHolder.query("BEGIN");

    await lockHolder.query(
      `
        SELECT product_id
        FROM inventory
        WHERE product_id = $1
        FOR UPDATE
      `,
      [productId],
    );

    logger.info({ productId }, "First transaction acquired inventory lock");

    let retryEvents = 0;

    try {
      await withTransactionRetry(
        pool,

        async (client) => {
          logger.info("Second transaction attempting lock");

          await client.query(
            `
              SELECT product_id
              FROM inventory
              WHERE product_id = $1
              FOR UPDATE
            `,
            [productId],
          );
        },

        {
          maxAttempts: 3,
          baseDelayMs: 100,
          lockTimeoutMs: 1_000,
          statementTimeoutMs: 5_000,

          onRetry(event) {
            retryEvents += 1;

            logger.warn(event, "Transaction retry triggered");
          },
        },
      );
    } catch (error) {
      const databaseError = error as PostgreSqlError;

      logger.warn(
        {
          errorCode: databaseError.code,
          errorMessage: databaseError.message,
          retryEvents,
        },
        "Second transaction failed",
      );
    }
  } finally {
    await lockHolder.query("ROLLBACK");
    lockHolder.release();

    await pool.end();
  }
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Lock timeout lab failed");

  process.exitCode = 1;
});
