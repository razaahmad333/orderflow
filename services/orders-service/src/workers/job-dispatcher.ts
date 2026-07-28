import "dotenv/config";

import { randomUUID } from "node:crypto";

import type { Queue } from "bullmq";

import { loadConfig } from "../config";
import { createDatabasePool } from "../database";
import {
  claimBackgroundJobs,
  markBackgroundJobEnqueued,
  rescheduleBackgroundJob,
} from "../jobs/background-job-outbox";

import {
  createBullProducerConnection,
  createNotificationQueue,
  notificationQueueName,
  type OrderConfirmedJob,
} from "../jobs/bullmq";

import { createLogger } from "../logger";

function calculateRetryDelay(attemptCount: number): number {
  const delay = 1000 * 2 ** Math.min(attemptCount, 8);

  const jitter = Math.floor(Math.random() * 500);

  return Math.min(delay + jitter, 300_000);
}

async function main(): Promise<void> {
  const config = loadConfig();

  const logger = createLogger({
    ...config,
    SERVICE_NAME: "job-dispatcher",
  });

  const pool = createDatabasePool(config, logger);

  const redisConnection = createBullProducerConnection(config);

  const notificationQueue = createNotificationQueue(config, redisConnection);

  const queues = new Map<string, Queue>([
    [notificationQueueName, notificationQueue],
  ]);

  const workerId = `job-dispatcher-${randomUUID()}`;

  let stopped = false;

  async function dispatchBatch(): Promise<void> {
    const events = await claimBackgroundJobs(pool, {
      workerId,
      batchSize: config.BACKGROUND_JOB_BATCH_SIZE,
      lockMs: config.BACKGROUND_JOB_LOCK_MS,
    });

    for (const event of events) {
      const queue = queues.get(event.queueName);

      if (!queue) {
        await rescheduleBackgroundJob(pool, {
          eventId: event.id,
          error: new Error(`Unknown queue ${event.queueName}`),
          delayMs: 60_000,
        });

        continue;
      }

      try {
        await queue.add(event.jobType, event.payload as OrderConfirmedJob, {
          /*
           * Re-dispatching the same outbox row uses
           * the same BullMQ job identity.
           */
          jobId: event.id,
        });

        await markBackgroundJobEnqueued(pool, event.id);

        logger.info(
          {
            outboxEventId: event.id,
            queueName: event.queueName,
            jobType: event.jobType,
          },
          "Background job enqueued",
        );
      } catch (error) {
        const delayMs = calculateRetryDelay(event.attemptCount);

        await rescheduleBackgroundJob(pool, {
          eventId: event.id,
          error,
          delayMs,
        });

        logger.warn(
          {
            err: error,
            outboxEventId: event.id,
            delayMs,
          },
          "Background job dispatch rescheduled",
        );
      }
    }
  }

  async function run(): Promise<void> {
    while (!stopped) {
      try {
        await dispatchBatch();
      } catch (error) {
        logger.error({ err: error }, "Job dispatcher cycle failed");
      }

      await new Promise((resolve) => {
        setTimeout(resolve, config.BACKGROUND_JOB_POLL_MS);
      });
    }
  }

  async function shutdown(signal: string): Promise<void> {
    if (stopped) {
      return;
    }

    stopped = true;

    logger.info({ signal }, "Stopping job dispatcher");

    await notificationQueue.close();
    await redisConnection.quit();
    await pool.end();

    logger.info("Job dispatcher stopped");
  }

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  await run();
}

main().catch((error: unknown) => {
  process.stderr.write(`${String(error)}\n`);

  process.exitCode = 1;
});
