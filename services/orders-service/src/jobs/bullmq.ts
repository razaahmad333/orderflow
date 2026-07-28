import IORedis from "ioredis";
import { Queue, type JobsOptions } from "bullmq";

import type { AppConfig } from "../config";

export const notificationQueueName = "notifications";

export interface OrderConfirmedJob {
  tenantId: string;
  orderId: string;
  externalId: string;
  totalMinor: string;
  currency: string;
}

export function createBullProducerConnection(config: AppConfig): IORedis {
  return new IORedis(config.REDIS_URL, {
    /*
     * Producers should fail within a bounded number
     * of command retries so the PostgreSQL outbox can
     * reschedule publishing.
     */
    maxRetriesPerRequest: 1,
    enableReadyCheck: true,
  });
}

export function createBullWorkerConnection(config: AppConfig): IORedis {
  return new IORedis(config.REDIS_URL, {
    /*
     * BullMQ workers require null because they use
     * blocking Redis commands.
     */
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  });
}

export function createNotificationQueue(
  config: AppConfig,
  connection: IORedis,
): Queue<OrderConfirmedJob> {
  const defaultJobOptions: JobsOptions = {
    attempts: config.NOTIFICATION_JOB_ATTEMPTS,

    backoff: {
      type: "exponential",
      delay: config.NOTIFICATION_JOB_BACKOFF_MS,
    },

    removeOnComplete: {
      age: 3600,
      count: 1000,
    },

    /*
     * Keep failed jobs for inspection.
     */
    removeOnFail: false,
  };

  return new Queue<OrderConfirmedJob>(notificationQueueName, {
    connection,
    defaultJobOptions,
  });
}
