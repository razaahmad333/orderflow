import "dotenv/config";

import { Kafka, logLevel } from "kafkajs";

import { z } from "zod";

import { orderCreatedEventSchema } from "../events/order-created-event";

const environmentSchema = z.object({
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

async function main(): Promise<void> {
  const environment = environmentSchema.parse(process.env);

  const kafka = new Kafka({
    clientId: "notification-worker",
    brokers: environment.KAFKA_BROKERS,
    logLevel: logLevel.WARN,
  });

  const consumer = kafka.consumer({
    groupId: environment.KAFKA_NOTIFICATION_GROUP_ID,
  });

  await consumer.connect();

  await consumer.subscribe({
    topics: [environment.KAFKA_ORDER_CREATED_TOPIC],

    fromBeginning: true,
  });

  process.stdout.write(
    JSON.stringify({
      level: "info",
      message: "Notification consumer connected",

      groupId: environment.KAFKA_NOTIFICATION_GROUP_ID,

      topic: environment.KAFKA_ORDER_CREATED_TOPIC,
    }) + "\n",
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        throw new Error("Kafka message value is empty");
      }

      const parsed = orderCreatedEventSchema.parse(
        JSON.parse(message.value.toString("utf8")),
      );

      /*
       * Simulated notification. We will later
       * replace this with an idempotent handler.
       */
      process.stdout.write(
        JSON.stringify({
          level: "info",
          message: "Order notification processed",

          topic,
          partition,
          offset: message.offset,

          eventId: parsed.eventId,
          tenantId: parsed.tenantId,
          orderId: parsed.orderId,
          externalId: parsed.externalId,
          totalMinor: parsed.totalMinor,
          currency: parsed.currency,
        }) + "\n",
      );
    },
  });

  let shuttingDown = false;

  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;

    process.stdout.write(
      JSON.stringify({
        level: "info",
        message: "Notification worker shutting down",
        signal,
      }) + "\n",
    );

    await consumer.disconnect();
  }

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
}

main().catch((error: unknown) => {
  process.stderr.write(
    JSON.stringify({
      level: "fatal",
      message: "Notification worker failed",
      error: error instanceof Error ? error.message : String(error),
    }) + "\n",
  );

  process.exitCode = 1;
});
