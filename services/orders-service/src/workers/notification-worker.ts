import "dotenv/config";

import { Job, Worker } from "bullmq";

import { loadConfig } from "../config";

import {
  createBullWorkerConnection,
  notificationQueueName,
  type OrderConfirmedJob,
} from "../jobs/bullmq";

import { createLogger } from "../logger";

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

      /*
       * Simulate an external provider call.
       */
      await delay(200);

      logger.info(
        {
          jobId: job.id,
          orderId: job.data.orderId,
        },
        "Order notification delivered",
      );

      return {
        delivered: true,
      };
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
