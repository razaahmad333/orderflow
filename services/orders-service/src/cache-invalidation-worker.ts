import { randomUUID } from "node:crypto";

import type { Pool } from "pg";
import type { Logger } from "pino";

import {
  claimCacheInvalidations,
  markCacheInvalidationProcessed,
  rescheduleCacheInvalidation,
} from "./cache-invalidation-outbox";

import type { ProductCache } from "./products/product-service";

interface RunBatchOptions {
  pool: Pool;
  cache: ProductCache;
  logger: Logger;
  workerId: string;
  batchSize: number;
  lockMs: number;
}

export interface CacheInvalidationWorker {
  stop(): Promise<void>;
}

function calculateRetryDelay(attemptCount: number): number {
  const baseDelayMs = 1_000;

  const exponentialDelay = baseDelayMs * 2 ** Math.min(attemptCount, 8);

  const jitter = Math.floor(Math.random() * 500);

  return Math.min(exponentialDelay + jitter, 300_000);
}

export async function runCacheInvalidationBatch({
  pool,
  cache,
  logger,
  workerId,
  batchSize,
  lockMs,
}: RunBatchOptions): Promise<number> {
  const events = await claimCacheInvalidations(pool, {
    workerId,
    batchSize,
    lockMs,
  });

  for (const event of events) {
    try {
      if (!cache.isReady) {
        throw new Error("Redis is not ready");
      }

      await cache.del(event.cacheKey);

      await markCacheInvalidationProcessed(pool, event.id);

      logger.info(
        {
          eventId: event.id,
          cacheKey: event.cacheKey,
        },
        "Cache invalidation processed",
      );
    } catch (error) {
      const delayMs = calculateRetryDelay(event.attemptCount);

      await rescheduleCacheInvalidation(pool, {
        eventId: event.id,
        error,
        delayMs,
      });

      logger.warn(
        {
          err: error,
          eventId: event.id,
          delayMs,
        },
        "Cache invalidation rescheduled",
      );
    }
  }

  return events.length;
}

export function startCacheInvalidationWorker(input: {
  pool: Pool;
  cache: ProductCache;
  logger: Logger;
  pollMs: number;
  batchSize: number;
  lockMs: number;
}): CacheInvalidationWorker {
  const workerId = `cache-invalidation-${randomUUID()}`;

  let stopped = false;
  let timer: NodeJS.Timeout | undefined;
  let currentRun: Promise<void> = Promise.resolve();

  async function tick(): Promise<void> {
    if (stopped) {
      return;
    }

    try {
      await runCacheInvalidationBatch({
        pool: input.pool,
        cache: input.cache,
        logger: input.logger,
        workerId,
        batchSize: input.batchSize,
        lockMs: input.lockMs,
      });
    } catch (error) {
      input.logger.error(
        { err: error },
        "Cache invalidation worker cycle failed",
      );
    }

    if (!stopped) {
      timer = setTimeout(() => {
        currentRun = tick();
      }, input.pollMs);

      timer.unref();
    }
  }

  currentRun = tick();

  return {
    async stop(): Promise<void> {
      stopped = true;

      if (timer) {
        clearTimeout(timer);
      }

      await currentRun;
    },
  };
}
