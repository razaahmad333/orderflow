import type { KafkaMessage, Producer } from "kafkajs";

const retryableErrorCodes = new Set([
  "40001", // serialization_failure
  "40P01", // deadlock_detected
  "55P03", // lock_not_available
  "57P01", // admin_shutdown
  "57P02", // crash_shutdown
  "57P03", // cannot_connect_now
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "EPIPE",
]);

interface ErrorWithCode {
  code?: unknown;
}

export interface KafkaRetryEnvelope {
  originalEventId: string;
  originalTopic: string;
  originalPartition: number;
  originalOffset: string;
  originalKey: string | null;
  originalPayload: string;
  consumerGroup: string;
  attempt: number;
  firstFailedAt: string;
  lastFailedAt: string;
  nextRetryAt: string;
  failureClass: "retryable";
  errorCode: string;
  errorMessage: string;
}

function getErrorCode(error: unknown): string {
  if (
    typeof error === "object" &&
    error !== null &&
    typeof (error as ErrorWithCode).code === "string"
  ) {
    return (error as ErrorWithCode).code as string;
  }

  return "UNKNOWN";
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function isRetryableKafkaProcessingError(error: unknown): boolean {
  return retryableErrorCodes.has(getErrorCode(error));
}

export function createKafkaRetryEnvelope(input: {
  eventId: string;
  consumerGroup: string;
  topic: string;
  partition: number;
  offset: string;
  message: KafkaMessage;
  error: unknown;
  retryDelayMs: number;
  failedAt?: Date;
}): KafkaRetryEnvelope {
  const failedAt = input.failedAt ?? new Date();

  return {
    originalEventId: input.eventId,
    originalTopic: input.topic,
    originalPartition: input.partition,
    originalOffset: input.offset,
    originalKey: input.message.key?.toString("utf8") ?? null,
    originalPayload: input.message.value?.toString("utf8") ?? "",
    consumerGroup: input.consumerGroup,
    attempt: 1,
    firstFailedAt: failedAt.toISOString(),
    lastFailedAt: failedAt.toISOString(),
    nextRetryAt: new Date(failedAt.getTime() + input.retryDelayMs).toISOString(),
    failureClass: "retryable",
    errorCode: getErrorCode(input.error),
    errorMessage: getErrorMessage(input.error),
  };
}

export async function publishKafkaRetry(
  producer: Producer,
  input: {
    retryTopic: string;
    envelope: KafkaRetryEnvelope;
  },
): Promise<void> {
  await producer.send({
    topic: input.retryTopic,
    acks: -1,
    messages: [
      {
        key:
          input.envelope.originalKey ?? input.envelope.originalEventId,
        value: JSON.stringify(input.envelope),
        headers: {
          "failure-class": input.envelope.failureClass,
          "retry-attempt": String(input.envelope.attempt),
          "next-retry-at": input.envelope.nextRetryAt,
          "original-topic": input.envelope.originalTopic,
          "original-partition": String(input.envelope.originalPartition),
          "original-offset": input.envelope.originalOffset,
          "consumer-group": input.envelope.consumerGroup,
        },
      },
    ],
  });
}
