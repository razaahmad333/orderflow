import type { IHeaders, KafkaMessage, Producer } from "kafkajs";
import type { Pool } from "pg";

export type KafkaPoisonErrorKind =
  | "empty_payload"
  | "invalid_json"
  | "schema_validation";

interface SerializableKafkaHeaders {
  [key: string]: string | string[];
}

export interface KafkaDeadLetterEnvelope {
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  originalKey: string | null;
  consumerGroup: string;
  failureClass: "non_retryable";
  errorCode: KafkaPoisonErrorKind;
  errorMessage: string;
  failedAt: string;
  originalPayload: string | null;
}

function normalizeHeaderValue(
  value: Buffer | string | (Buffer | string)[] | undefined,
): string | string[] | null {
  if (value === undefined) {
    return null;
  }

  if (Array.isArray(value)) {
    return value.map((entry) =>
      typeof entry === "string" ? entry : entry.toString("utf8"),
    );
  }

  return typeof value === "string" ? value : value.toString("utf8");
}

export function serializeKafkaHeaders(
  headers: IHeaders | undefined,
): SerializableKafkaHeaders {
  if (!headers) {
    return {};
  }

  const serialized: SerializableKafkaHeaders = {};

  for (const [key, value] of Object.entries(headers)) {
    const normalized = normalizeHeaderValue(value);

    if (normalized !== null) {
      serialized[key] = normalized;
    }
  }

  return serialized;
}

export function createKafkaDeadLetterEnvelope(input: {
  consumerGroup: string;
  topic: string;
  partition: number;
  offset: string;
  message: KafkaMessage;
  errorKind: KafkaPoisonErrorKind;
  errorMessage: string;
  failedAt?: string;
}): KafkaDeadLetterEnvelope {
  return {
    originalTopic: input.topic,
    originalPartition: input.partition,
    originalOffset: input.offset,
    originalKey: input.message.key?.toString("utf8") ?? null,
    consumerGroup: input.consumerGroup,
    failureClass: "non_retryable",
    errorCode: input.errorKind,
    errorMessage: input.errorMessage,
    failedAt: input.failedAt ?? new Date().toISOString(),
    originalPayload: input.message.value?.toString("utf8") ?? null,
  };
}

export async function publishKafkaDeadLetter(
  producer: Producer,
  input: {
    dlqTopic: string;
    envelope: KafkaDeadLetterEnvelope;
  },
): Promise<void> {
  const sourcePosition = [
    input.envelope.consumerGroup,
    input.envelope.originalTopic,
    input.envelope.originalPartition,
    input.envelope.originalOffset,
  ].join(":");

  await producer.send({
    topic: input.dlqTopic,
    acks: -1,
    messages: [
      {
        key: sourcePosition,
        value: JSON.stringify(input.envelope),
        headers: {
          "failure-class": input.envelope.failureClass,
          "error-code": input.envelope.errorCode,
          "original-topic": input.envelope.originalTopic,
          "original-partition": String(input.envelope.originalPartition),
          "original-offset": input.envelope.originalOffset,
          "consumer-group": input.envelope.consumerGroup,
        },
      },
    ],
  });
}

export async function recordPoisonKafkaMessage(
  pool: Pool,
  input: {
    consumerName: string;
    topic: string;
    partition: number;
    offset: string;
    message: KafkaMessage;
    errorKind: KafkaPoisonErrorKind;
    errorMessage: string;
  },
): Promise<void> {
  await pool.query(
    `
      INSERT INTO kafka_dead_letter (
        consumer_name,
        topic,
        partition_number,
        message_offset,
        key_text,
        payload_text,
        headers,
        error_kind,
        error_message
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7::JSONB,
        $8,
        $9
      )
      ON CONFLICT DO NOTHING
    `,
    [
      input.consumerName,
      input.topic,
      input.partition,
      input.offset,
      input.message.key?.toString("utf8") ?? null,
      input.message.value?.toString("utf8") ?? null,
      JSON.stringify(serializeKafkaHeaders(input.message.headers)),
      input.errorKind,
      input.errorMessage,
    ],
  );
}
