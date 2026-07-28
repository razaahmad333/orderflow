import "dotenv/config";

import { randomUUID } from "node:crypto";

import { Kafka, logLevel } from "kafkajs";

import pino from "pino";
import { Pool } from "pg";
import { z } from "zod";

import {
  claimOutboxEvents,
  markOutboxEventPublished,
  rescheduleOutboxEvent,
} from "../outbox/kafka-outbox";

const environmentSchema = z.object({
  DATABASE_URL: z.string().url(),

  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) =>
      value
        .split(",")
        .map((broker) => broker.trim())
        .filter(Boolean),
    ),

  KAFKA_OUTBOX_POLL_MS: z.coerce.number().int().min(100).default(1000),

  KAFKA_OUTBOX_BATCH_SIZE: z.coerce.number().int().min(1).max(100).default(20),

  KAFKA_OUTBOX_LOCK_MS: z.coerce.number().int().min(1000).default(30000),
});

function calculateRetryDelay(attemptCount: number): number {
  const exponential = 1000 * 2 ** Math.min(attemptCount, 8);

  const jitter = Math.floor(Math.random() * 500);

  return Math.min(exponential + jitter, 300_000);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function main(): Promise<void> {
  const environment = environmentSchema.parse(process.env);

  const logger = pino({
    level: "info",

    base: {
      service: "order-outbox-publisher",
    },

    timestamp: pino.stdTimeFunctions.isoTime,
  });

  const workerId = `outbox-${randomUUID()}`;

  const pool = new Pool({
    connectionString: environment.DATABASE_URL,

    max: 5,

    application_name: "order-outbox-publisher",
  });

  const kafka = new Kafka({
    clientId: "order-outbox-publisher",

    brokers: environment.KAFKA_BROKERS,

    logLevel: logLevel.WARN,
  });

  const producer = kafka.producer({
    allowAutoTopicCreation: false,
  });

  await producer.connect();

  logger.info({ workerId }, "Outbox publisher started");

  let stopping = false;

  async function shutdown(signal: string): Promise<void> {
    if (stopping) {
      return;
    }

    stopping = true;

    logger.info({ signal }, "Outbox publisher stopping");
  }

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  try {
    while (!stopping) {
      const events = await claimOutboxEvents(pool, {
        workerId,

        batchSize: environment.KAFKA_OUTBOX_BATCH_SIZE,

        lockTimeoutMs: environment.KAFKA_OUTBOX_LOCK_MS,
      });

      for (const event of events) {
        try {
          await producer.send({
            topic: event.topic,
            acks: -1,

            messages: [
              {
                key: event.partitionKey,

                value: JSON.stringify(event.payload),

                headers: {
                  "event-id": event.id,

                  "event-type": event.eventType,

                  "event-version": String(event.eventVersion),
                },
              },
            ],
          });

          await markOutboxEventPublished(pool, event.id);

          logger.info(
            {
              eventId: event.id,
              eventType: event.eventType,
            },
            "Outbox event published",
          );
        } catch (error) {
          const delayMs = calculateRetryDelay(event.attemptCount);

          await rescheduleOutboxEvent(pool, {
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
            "Outbox event rescheduled",
          );
        }
      }

      if (events.length === 0) {
        await delay(environment.KAFKA_OUTBOX_POLL_MS);
      }
    }
  } finally {
    await producer.disconnect();
    await pool.end();

    logger.info("Outbox publisher stopped");
  }
}

main().catch((error: unknown) => {
  process.stderr.write(
    JSON.stringify({
      level: "fatal",

      message: "Outbox publisher failed",

      error: error instanceof Error ? error.message : String(error),
    }) + "\n",
  );

  process.exitCode = 1;
});
