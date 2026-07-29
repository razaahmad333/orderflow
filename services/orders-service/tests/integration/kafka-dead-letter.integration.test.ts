import type { KafkaMessage } from "kafkajs";
import type { Pool } from "pg";

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import {
  recordPoisonKafkaMessage,
  serializeKafkaHeaders,
} from "../../src/consumers/kafka-dead-letter";
import {
  createTestPool,
  migrateTestDatabase,
  resetTestDatabase,
} from "./test-database";

let pool: Pool;

function createKafkaMessage(input: {
  key: string;
  value: string;
  headers: Record<string, Buffer | Buffer[]>;
  offset: string;
}): KafkaMessage {
  return {
    key: Buffer.from(input.key),
    value: Buffer.from(input.value),
    headers: input.headers,
    timestamp: "0",
    attributes: 0,
    offset: input.offset,
  } as KafkaMessage;
}

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);

  await pool.query("TRUNCATE kafka_dead_letter");
});

afterAll(async () => {
  await pool.end();
});

describe("kafka dead letter storage", () => {
  it("stores one poison record with payload, key, and headers", async () => {
    await recordPoisonKafkaMessage(pool, {
      consumerName: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "17",
      message: createKafkaMessage({
        key: "order-17",
        value: "{\"broken\":true}",
        headers: {
          "event-type": Buffer.from("order.created"),
          "x-trace-id": Buffer.from("trace-17"),
        },
        offset: "17",
      }),
      errorKind: "schema_validation",
      errorMessage: "orderId is required",
    });

    const result = await pool.query<{
      key_text: string | null;
      payload_text: string | null;
      headers: Record<string, string>;
      error_kind: string;
      error_message: string;
    }>(
      `
        SELECT
          key_text,
          payload_text,
          headers,
          error_kind,
          error_message
        FROM kafka_dead_letter
      `,
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toEqual({
      key_text: "order-17",
      payload_text: "{\"broken\":true}",
      headers: {
        "event-type": "order.created",
        "x-trace-id": "trace-17",
      },
      error_kind: "schema_validation",
      error_message: "orderId is required",
    });
  });

  it("deduplicates the same poison Kafka position", async () => {
    const message = createKafkaMessage({
      key: "order-18",
      value: "not-json",
      headers: {
        "event-type": Buffer.from("order.created"),
      },
      offset: "18",
    });

    await recordPoisonKafkaMessage(pool, {
      consumerName: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "18",
      message,
      errorKind: "invalid_json",
      errorMessage: "Unexpected token",
    });

    await recordPoisonKafkaMessage(pool, {
      consumerName: "notification-service-v1",
      topic: "order.created.v1",
      partition: 2,
      offset: "18",
      message,
      errorKind: "invalid_json",
      errorMessage: "Unexpected token",
    });

    const result = await pool.query<{ count: string }>(
      `
        SELECT COUNT(*) AS count
        FROM kafka_dead_letter
      `,
    );

    expect(result.rows[0]?.count).toBe("1");
  });

  it("serializes repeated Kafka headers deterministically", () => {
    expect(
      serializeKafkaHeaders({
        retry: [Buffer.from("1"), Buffer.from("2")],
        single: Buffer.from("value"),
      }),
    ).toEqual({
      retry: ["1", "2"],
      single: "value",
    });
  });
});
