import type { IHeaders, KafkaMessage } from "kafkajs";
import type { Pool } from "pg";

export type KafkaPoisonErrorKind =
  | "empty_payload"
  | "invalid_json"
  | "schema_validation";

interface SerializableKafkaHeaders {
  [key: string]: string | string[];
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
