import { randomInt } from "node:crypto";

import type { Pool, PoolClient } from "pg";

const retryableTransactionCodes = new Set([
  "40P01", // deadlock_detected
  "40001", // serialization_failure
]);

interface PostgresError {
  code?: string;
}

export interface TransactionRetryEvent {
  attempt: number;
  nextAttempt: number;
  maxAttempts: number;
  delayMs: number;
  errorCode: string;
}

export interface TransactionOptions {
  maxAttempts?: number;
  baseDelayMs?: number;

  lockTimeoutMs?: number;
  statementTimeoutMs?: number;

  onRetry?: (event: TransactionRetryEvent) => void;
}


function isRetryableTransactionError(
  error: unknown,
): error is PostgresError & { code: string } {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return false;
  }

  const code = (error as PostgresError).code;

  return typeof code === "string" && retryableTransactionCodes.has(code);
}

function calculateRetryDelay(attempt: number, baseDelayMs: number): number {
  const exponentialDelay = baseDelayMs * 2 ** (attempt - 1);

  const jitter = randomInt(0, baseDelayMs + 1);

  return exponentialDelay + jitter;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function configureTransactionTimeouts(
  client: PoolClient,
  options: TransactionOptions,
): Promise<void> {
  const lockTimeoutMs = options.lockTimeoutMs ?? 0;

  const statementTimeoutMs = options.statementTimeoutMs ?? 0;

  await client.query(
    `
      SELECT
        set_config(
          'lock_timeout',
          $1,
          true
        ),
        set_config(
          'statement_timeout',
          $2,
          true
        )
    `,
    [`${lockTimeoutMs}ms`, `${statementTimeoutMs}ms`],
  );
}

export async function withTransactionRetry<T>(
  pool: Pool,
  operation: (client: PoolClient, attempt: number) => Promise<T>,
  options: TransactionOptions = {},
): Promise<T> {
  const maxAttempts = options.maxAttempts ?? 3;
  const baseDelayMs = options.baseDelayMs ?? 50;

  if (maxAttempts < 1) {
    throw new Error("Transaction maxAttempts must be at least 1");
  }

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const client = await pool.connect();
    let retryDelayMs: number | undefined;

    try {
      await client.query("BEGIN");

      await configureTransactionTimeouts(client, options);

      const result = await operation(client, attempt);

      await client.query("COMMIT");

      return result;
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        throw new AggregateError(
          [error, rollbackError],
          "Transaction and rollback both failed",
        );
      }

      const canRetry =
        isRetryableTransactionError(error) && attempt < maxAttempts;

      if (!canRetry) {
        throw error;
      }

      retryDelayMs = calculateRetryDelay(attempt, baseDelayMs);

      options.onRetry?.({
        attempt,
        nextAttempt: attempt + 1,
        maxAttempts,
        delayMs: retryDelayMs,
        errorCode: error.code,
      });
    } finally {
      client.release();
    }

    if (retryDelayMs !== undefined) {
      await sleep(retryDelayMs);
    }
  }

  throw new Error("Transaction retry loop ended unexpectedly");
}
