import "dotenv/config";

import { Kafka, logLevel, type KafkaMessage, type Consumer } from "kafkajs";

import pino from "pino";
import { Pool } from "pg";
import { ZodError, z } from "zod";

import {
  recordPoisonKafkaMessage,
  type KafkaPoisonErrorKind,
} from "../consumers/kafka-dead-letter";
import { processOrderCreatedEvent } from "../consumers/notification-consumer-inbox";
import { orderCreatedEventSchema } from "../events/order-created-event";

const environmentSchema = z.object({
  DATABASE_URL: z.string().url(),

  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"])
    .default("info"),

  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((value) =>
      value
        .split(",")
        .map((broker) => broker.trim())
        .filter(Boolean),
    ),

  KAFKA_ORDER_CREATED_TOPIC: z.string().default("order.created.v1"),

  KAFKA_NOTIFICATION_GROUP_ID: z.string().default("notification-service-v1"),
});

function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
}

function getErrorMessage(error: unknown): string {
  if (error instanceof ZodError) {
    return error.issues
      .map((issue) => `${issue.path.join(".") || "<root>"}: ${issue.message}`)
      .join("; ");
  }

  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

async function deadLetterAndCommit(input: {
  pool: Pool;
  consumer: Consumer;
  logger: pino.Logger;
  consumerName: string;
  topic: string;
  partition: number;
  offset: string;
  message: KafkaMessage;
  errorKind: KafkaPoisonErrorKind;
  error: unknown;
  resolveOffset: (offset: string) => void;
}): Promise<void> {
  const committedOffset = nextOffset(input.offset);

  await recordPoisonKafkaMessage(input.pool, {
    consumerName: input.consumerName,
    topic: input.topic,
    partition: input.partition,
    offset: input.offset,
    message: input.message,
    errorKind: input.errorKind,
    errorMessage: getErrorMessage(input.error),
  });

  await input.consumer.commitOffsets([
    {
      topic: input.topic,
      partition: input.partition,
      offset: committedOffset,
    },
  ]);

  input.resolveOffset(input.offset);

  input.logger.warn(
    {
      err: input.error,
      topic: input.topic,
      partition: input.partition,
      offset: input.offset,
      committedOffset,
      consumerName: input.consumerName,
      errorKind: input.errorKind,
    },
    "Poison Kafka message dead-lettered and offset committed",
  );
}

async function main(): Promise<void> {
  const environment = environmentSchema.parse(process.env);

  const logger = pino({
    level: environment.LOG_LEVEL,
    base: {
      service: "notification-kafka-worker",
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });

  const kafka = new Kafka({
    clientId: "notification-kafka-worker",
    brokers: environment.KAFKA_BROKERS,
    logLevel: logLevel.WARN,
  });

  const consumer = kafka.consumer({
    groupId: environment.KAFKA_NOTIFICATION_GROUP_ID,
  });

  const pool = new Pool({
    connectionString: environment.DATABASE_URL,
    max: 5,
    application_name: "notification-kafka-worker",
  });

  await consumer.connect();

  await consumer.subscribe({
    topics: [environment.KAFKA_ORDER_CREATED_TOPIC],
    fromBeginning: true,
  });

  logger.info(
    {
      consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
      topic: environment.KAFKA_ORDER_CREATED_TOPIC,
    },
    "Notification Kafka consumer connected",
  );

  let shuttingDown = false;

  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;

    logger.info({ signal }, "Notification Kafka worker shutting down");

    await consumer.disconnect();
    await pool.end();
  }

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      heartbeat,
      isRunning,
      isStale,
      resolveOffset,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          break;
        }

        if (!message.value) {
          await deadLetterAndCommit({
            pool,
            consumer,
            logger,
            consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset,
            message,
            errorKind: "empty_payload",
            error: new Error("Kafka message value is empty"),
            resolveOffset,
          });

          await heartbeat();

          continue;
        }

        let parsed;

        try {
          parsed = JSON.parse(message.value.toString("utf8"));
        } catch (error) {
          await deadLetterAndCommit({
            pool,
            consumer,
            logger,
            consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset,
            message,
            errorKind: "invalid_json",
            error,
            resolveOffset,
          });

          await heartbeat();

          continue;
        }

        let validatedEvent;

        try {
          validatedEvent = orderCreatedEventSchema.parse(parsed);
        } catch (error) {
          await deadLetterAndCommit({
            pool,
            consumer,
            logger,
            consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset,
            message,
            errorKind: "schema_validation",
            error,
            resolveOffset,
          });

          await heartbeat();

          continue;
        }

        try {
          const eventId = validatedEvent.eventId;
          const orderId = validatedEvent.orderId;

          const client = await pool.connect();

          try {
            await client.query("BEGIN");

            const result = await processOrderCreatedEvent({
              client,
              consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
              topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
              event: validatedEvent,
            });

            await client.query("COMMIT");

            const committedOffset = nextOffset(message.offset);

            await consumer.commitOffsets([
              {
                topic: batch.topic,
                partition: batch.partition,
                offset: committedOffset,
              },
            ]);

            resolveOffset(message.offset);

            logger.info(
              {
                eventId: validatedEvent.eventId,
                orderId: validatedEvent.orderId,
                topic: batch.topic,
                partition: batch.partition,
                offset: message.offset,
                consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
                result,
              },
              result === "processed"
                ? "Kafka event processed"
                : "Duplicate Kafka event skipped",
            );

            logger.info(
              {
                eventId: validatedEvent.eventId,
                orderId: validatedEvent.orderId,
                topic: batch.topic,
                partition: batch.partition,
                offset: committedOffset,
                consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
              },
              "Kafka offset committed",
            );

            await heartbeat();
          } catch (error) {
            try {
              await client.query("ROLLBACK");
            } catch (rollbackError) {
              logger.error(
                {
                  err: rollbackError,
                  topic: batch.topic,
                  partition: batch.partition,
                  offset: message.offset,
                  consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
                },
                "Kafka consumer rollback failed",
              );
            }

            throw error;
          } finally {
            client.release();
          }
        } catch (error) {
          logger.error(
            {
              err: error,
              eventId: validatedEvent.eventId,
              orderId: validatedEvent.orderId,
              topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
              consumerName: environment.KAFKA_NOTIFICATION_GROUP_ID,
            },
            "Kafka consumer processing failure",
          );

          throw error;
        }
      }
    },
  });
}

main().catch((error: unknown) => {
  process.stderr.write(
    JSON.stringify({
      level: "fatal",
      message: "Notification Kafka worker failed",
      error: error instanceof Error ? error.message : String(error),
    }) + "\n",
  );

  process.exitCode = 1;
});
