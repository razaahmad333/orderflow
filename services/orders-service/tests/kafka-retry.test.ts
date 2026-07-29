import type { KafkaMessage, Producer } from "kafkajs";

import { describe, expect, it, vi } from "vitest";

import {
  createKafkaRetryEnvelope,
  isRetryableKafkaProcessingError,
  publishKafkaRetry,
} from "../src/consumers/kafka-retry";

function createMessage(): KafkaMessage {
  return {
    key: Buffer.from("order-42"),
    value: Buffer.from('{"eventId":"event-42"}'),
    timestamp: "0",
    attributes: 0,
    offset: "9",
  } as KafkaMessage;
}

describe("Kafka retry routing", () => {
  it.each(["40001", "40P01", "57P03", "ECONNREFUSED", "ECONNRESET"])(
    "classifies %s as retryable",
    (code) => {
      expect(
        isRetryableKafkaProcessingError(
          Object.assign(new Error("temporary failure"), { code }),
        ),
      ).toBe(true);
    },
  );

  it("does not classify unknown application errors as retryable", () => {
    expect(
      isRetryableKafkaProcessingError(new Error("programming defect")),
    ).toBe(false);
  });

  it("creates a first-attempt envelope with a delayed retry time", () => {
    const envelope = createKafkaRetryEnvelope({
      eventId: "event-42",
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 1,
      offset: "9",
      message: createMessage(),
      error: Object.assign(new Error("database restarting"), {
        code: "57P03",
      }),
      retryDelayMs: 10_000,
      failedAt: new Date("2026-07-29T00:00:00.000Z"),
    });

    expect(envelope).toMatchObject({
      originalEventId: "event-42",
      originalTopic: "order.created.v1",
      originalPartition: 1,
      originalOffset: "9",
      originalKey: "order-42",
      consumerGroup: "notification-service-v1",
      attempt: 1,
      firstFailedAt: "2026-07-29T00:00:00.000Z",
      lastFailedAt: "2026-07-29T00:00:00.000Z",
      nextRetryAt: "2026-07-29T00:00:10.000Z",
      failureClass: "retryable",
      errorCode: "57P03",
      errorMessage: "database restarting",
    });
  });

  it("publishes with full acknowledgement and the original ordering key", async () => {
    const send = vi.fn().mockResolvedValue([]);
    const producer = { send } as unknown as Producer;
    const envelope = createKafkaRetryEnvelope({
      eventId: "event-42",
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 1,
      offset: "9",
      message: createMessage(),
      error: Object.assign(new Error("connection reset"), {
        code: "ECONNRESET",
      }),
      retryDelayMs: 10_000,
    });

    await publishKafkaRetry(producer, {
      retryTopic: "order.created.v1.retry.10s",
      envelope,
    });

    expect(send).toHaveBeenCalledWith({
      topic: "order.created.v1.retry.10s",
      acks: -1,
      messages: [
        expect.objectContaining({
          key: "order-42",
          value: JSON.stringify(envelope),
        }),
      ],
    });
  });

  it("propagates publication failure so the source offset cannot be committed", async () => {
    const producer = {
      send: vi.fn().mockRejectedValue(new Error("Kafka unavailable")),
    } as unknown as Producer;
    const envelope = createKafkaRetryEnvelope({
      eventId: "event-42",
      consumerGroup: "notification-service-v1",
      topic: "order.created.v1",
      partition: 1,
      offset: "9",
      message: createMessage(),
      error: Object.assign(new Error("database unavailable"), {
        code: "ECONNREFUSED",
      }),
      retryDelayMs: 10_000,
    });

    await expect(
      publishKafkaRetry(producer, {
        retryTopic: "order.created.v1.retry.10s",
        envelope,
      }),
    ).rejects.toThrow("Kafka unavailable");
  });
});
