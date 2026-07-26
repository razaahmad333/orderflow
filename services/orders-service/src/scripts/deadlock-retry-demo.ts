import "dotenv/config";

import pino from "pino";
import { Pool } from "pg";

import { withTransactionRetry } from "../transaction";

const keyboardProductId = "10000000-0000-4000-8000-000000000001";

const mouseProductId = "10000000-0000-4000-8000-000000000002";

const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: {
    lab: "deadlock-retry",
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

function createBarrier(participantCount: number) {
  let arrivals = 0;
  let releaseBarrier!: () => void;

  const barrier = new Promise<void>((resolve) => {
    releaseBarrier = resolve;
  });

  return async function waitAtBarrier(): Promise<void> {
    arrivals += 1;

    if (arrivals === participantCount) {
      releaseBarrier();
    }

    await barrier;
  };
}

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const pool = new Pool({
    connectionString: databaseUrl,
    max: 4,
    application_name: "deadlock-retry-lab",
  });

  const firstAttemptBarrier = createBarrier(2);

  async function lockProducts(
    transactionName: string,
    firstProductId: string,
    secondProductId: string,
  ): Promise<void> {
    await withTransactionRetry(
      pool,
      async (client, attempt) => {
        logger.info(
          {
            transactionName,
            attempt,
            productId: firstProductId,
          },
          "Locking first product",
        );

        await client.query(
          `
            SELECT product_id
            FROM inventory
            WHERE product_id = $1
            FOR UPDATE
          `,
          [firstProductId],
        );

        logger.info(
          {
            transactionName,
            attempt,
            productId: firstProductId,
          },
          "First product locked",
        );

        /*
         * Force both first attempts to hold one
         * different lock before either requests
         * the second lock.
         */
        if (attempt === 1) {
          await firstAttemptBarrier();
        }

        logger.info(
          {
            transactionName,
            attempt,
            productId: secondProductId,
          },
          "Locking second product",
        );

        await client.query(
          `
            SELECT product_id
            FROM inventory
            WHERE product_id = $1
            FOR UPDATE
          `,
          [secondProductId],
        );

        logger.info(
          {
            transactionName,
            attempt,
          },
          "Both products locked",
        );
      },
      {
        maxAttempts: 3,
        baseDelayMs: 100,

        onRetry(event) {
          logger.warn(
            {
              transactionName,
              ...event,
            },
            "Retrying transaction",
          );
        },
      },
    );

    logger.info(
      {
        transactionName,
      },
      "Transaction completed",
    );
  }

  try {
    await Promise.all([
      lockProducts("transaction-a", keyboardProductId, mouseProductId),

      lockProducts("transaction-b", mouseProductId, keyboardProductId),
    ]);

    logger.info("Both transactions eventually completed");
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  logger.fatal(
    {
      err: error,
    },
    "Deadlock retry lab failed",
  );

  process.exitCode = 1;
});
