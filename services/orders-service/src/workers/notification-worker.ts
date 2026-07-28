import "dotenv/config";

import { Job, Worker } from "bullmq";

import { loadConfig } from "../config";

import {
  createBullWorkerConnection,
  notificationQueueName,
  type OrderConfirmedJob,
} from "../jobs/bullmq";

import { createLogger } from "../logger";
import { randomUUID } from "node:crypto";

import { createDatabasePool } from "../database";

import { processOrderConfirmedNotification } from "../notifications/order-confirmed-processor";
function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function main(): Promise<void> {
  const config = loadConfig();

  const logger = createLogger({
    ...config,
    SERVICE_NAME: "notification-worker",
  });
  const pool = createDatabasePool(config, logger);

  const workerInstanceId = `notification-worker-${randomUUID()}`;
  const connection = createBullWorkerConnection(config);

  const worker = new Worker<OrderConfirmedJob>(
    notificationQueueName,

    async (job: Job<OrderConfirmedJob>) => {
      logger.info(
        {
          jobId: job.id,
          jobName: job.name,
          attempt: job.attemptsMade + 1,
          orderId: job.data.orderId,
          externalId: job.data.externalId,
        },
        "Processing notification job",
      );

      if (job.attemptsMade < config.SIMULATE_NOTIFICATION_FAILURES) {
        throw new Error("Simulated notification provider failure");
      }

      const jobId = job.id;

      if (!jobId) {
        throw new Error("Notification job ID is required");
      }

      const result = await processOrderConfirmedNotification({
        pool,
        logger,

        jobId,

        data: job.data,

        /*
         * Crash only on the first BullMQ attempt.
         * The retry must be allowed to finish.
         */
        simulateCrashAfterProviderSuccess:
          config.SIMULATE_NOTIFICATION_CRASH_AFTER_SEND === 1 &&
          job.attemptsMade === 0,
      });

      logger.info(
        {
          workerInstanceId,
          jobId,
          orderId: job.data.orderId,
          skipped: result.skipped,
          providerCreatedNow: result.providerCreatedNow,
          providerMessageId: result.providerMessageId,
        },
        "Order notification processing completed",
      );

      return result;
    },

    {
      connection,

      concurrency: config.NOTIFICATION_WORKER_CONCURRENCY,
    },
  );

  worker.on("completed", (job) => {
    logger.info(
      {
        jobId: job.id,
        attemptsMade: job.attemptsMade,
      },
      "Notification job completed",
    );
  });

  worker.on("failed", (job, error) => {
    logger.warn(
      {
        err: error,
        jobId: job?.id,
        attemptsMade: job?.attemptsMade,
      },
      "Notification job attempt failed",
    );
  });

  async function shutdown(signal: string): Promise<void> {
    logger.info({ signal }, "Stopping notification worker");

    await worker.close();
    await connection.quit();
await pool.end();
    logger.info("Notification worker stopped");
  }

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  logger.info(
    {
      queue: notificationQueueName,
      concurrency: config.NOTIFICATION_WORKER_CONCURRENCY,
    },
    "Notification worker started",
  );
}

void main();
