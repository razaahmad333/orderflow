import type { KafkaMessage, Producer } from "kafkajs";

import { describe, expect, it, vi } from "vitest";

import {
  createKafkaDeadLetterEnvelope,
  publishKafkaDeadLetter,
} from "../src/consumers/kafka-dead-letter";

function createMessage(): KafkaMessage {
  return {
    key: Buffer.from("order-42"),
    value: Buffer.from("not-json"),
    timestamp: "0",
    attributes: 0,
    offset: "42",
  } as KafkaMessage;
}

describe("Kafka DLQ publication", () => {
  it("preserves the source position and failure details in the envelope", () => {
    const envelope = createKafkaDeadLetterEnvelope({
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "42",
      message: createMessage(),
      errorKind: "invalid_json",
      errorMessage: "Unexpected token",
      failedAt: "2026-07-29T00:00:00.000Z",
    });

    expect(envelope).toEqual({
      originalTopic: "order.created.v1",
      originalPartition: 2,
      originalOffset: "42",
      originalKey: "order-42",
      consumerGroup: "notification-service-v1",
      failureClass: "non_retryable",
      errorCode: "invalid_json",
      errorMessage: "Unexpected token",
      failedAt: "2026-07-29T00:00:00.000Z",
      originalPayload: "not-json",
    });
  });

  it("waits for an acknowledged DLQ publication", async () => {
    const send = vi.fn().mockResolvedValue([]);
    const producer = { send } as unknown as Producer;
    const envelope = createKafkaDeadLetterEnvelope({
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "42",
      message: createMessage(),
      errorKind: "invalid_json",
      errorMessage: "Unexpected token",
    });

    await publishKafkaDeadLetter(producer, {
      dlqTopic: "order.created.v1.dlq",
      envelope,
    });

    expect(send).toHaveBeenCalledWith({
      topic: "order.created.v1.dlq",
      acks: -1,
      messages: [
        expect.objectContaining({
          key: "notification-service-v1:order.created.v1:2:42",
          value: JSON.stringify(envelope),
        }),
      ],
    });
  });

  it("propagates publication failure so the caller cannot commit the offset", async () => {
    const producer = {
      send: vi.fn().mockRejectedValue(new Error("Kafka unavailable")),
    } as unknown as Producer;
    const envelope = createKafkaDeadLetterEnvelope({
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "42",
      message: createMessage(),
      errorKind: "invalid_json",
      errorMessage: "Unexpected token",
    });

    await expect(
      publishKafkaDeadLetter(producer, {
        dlqTopic: "order.created.v1.dlq",
        envelope,
      }),
    ).rejects.toThrow("Kafka unavailable");
  });
});
