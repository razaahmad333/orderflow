import { Kafka, logLevel, type Producer } from "kafkajs";

import type { AppConfig } from "./config";

export function createKafkaProducer(config: AppConfig): Producer {
  const kafka = new Kafka({
    clientId: config.KAFKA_CLIENT_ID,
    brokers: config.KAFKA_BROKERS,

    logLevel: logLevel.WARN,

    retry: {
      initialRetryTime: 100,
      maxRetryTime: 1_000,
      retries: 3,
    },
  });

  return kafka.producer({
    allowAutoTopicCreation: false,
  });
}
