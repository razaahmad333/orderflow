import type { Pool, PoolClient } from "pg";

import {
  createOrderCreatedEvent,
  type OrderCreatedEvent,
} from "../events/order-created-event";

interface EnqueueOrderCreatedInput {
  topic: string;
  tenantId: string;
  orderId: string;
  externalId: string;
  totalMinor: string;
  currency: string;

  items: Array<{
    productId: string;
    quantity: number;
  }>;
}

export interface ClaimedOutboxEvent {
  id: string;
  topic: string;
  partitionKey: string;
  eventType: string;
  eventVersion: number;
  payload: unknown;
  attemptCount: number;
}

interface ClaimedOutboxRow {
  id: string;
  topic: string;
  partition_key: string;
  event_type: string;
  event_version: number;
  payload: unknown;
  attempt_count: number;
}

export async function enqueueOrderCreatedEvent(
  client: PoolClient,
  input: EnqueueOrderCreatedInput,
): Promise<OrderCreatedEvent> {
  const event = createOrderCreatedEvent({
    tenantId: input.tenantId,
    orderId: input.orderId,
    externalId: input.externalId,
    totalMinor: input.totalMinor,
    currency: input.currency,
    items: input.items,
  });

  await client.query(
    `
      INSERT INTO kafka_outbox (
        id,
        aggregate_type,
        aggregate_id,
        event_type,
        event_version,
        topic,
        partition_key,
        payload
      )
      VALUES (
        $1,
        'order',
        $2,
        $3,
        $4,
        $5,
        $6,
        $7::JSONB
      )
    `,
    [
      event.eventId,
      event.orderId,
      event.eventType,
      event.eventVersion,
      input.topic,
      event.orderId,
      JSON.stringify(event),
    ],
  );

  return event;
}

export async function claimOutboxEvents(
  pool: Pool,
  input: {
    workerId: string;
    batchSize: number;
    lockTimeoutMs: number;
  },
): Promise<ClaimedOutboxEvent[]> {
  const result = await pool.query<ClaimedOutboxRow>(
    `
        WITH candidates AS (
          SELECT id
          FROM kafka_outbox
          WHERE published_at IS NULL
            AND next_attempt_at <= NOW()
            AND (
              locked_at IS NULL
              OR locked_at <
                NOW() -
                (
                  $1::INTEGER *
                  INTERVAL '1 millisecond'
                )
            )
          ORDER BY
            next_attempt_at,
            created_at
          FOR UPDATE SKIP LOCKED
          LIMIT $2
        )

        UPDATE kafka_outbox AS outbox
        SET
          status = 'processing',
          locked_at = NOW(),
          locked_by = $3
        FROM candidates
        WHERE outbox.id = candidates.id
        RETURNING
          outbox.id,
          outbox.topic,
          outbox.partition_key,
          outbox.event_type,
          outbox.event_version,
          outbox.payload,
          outbox.attempt_count
      `,
    [input.lockTimeoutMs, input.batchSize, input.workerId],
  );

  return result.rows.map((row) => ({
    id: row.id,
    topic: row.topic,
    partitionKey: row.partition_key,
    eventType: row.event_type,
    eventVersion: row.event_version,
    payload: row.payload,
    attemptCount: row.attempt_count,
  }));
}

export async function markOutboxEventPublished(
  pool: Pool,
  eventId: string,
): Promise<void> {
  await pool.query(
    `
      UPDATE kafka_outbox
      SET
        status = 'published',
        published_at = NOW(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL
      WHERE id = $1
    `,
    [eventId],
  );
}

export async function rescheduleOutboxEvent(
  pool: Pool,
  input: {
    eventId: string;
    error: unknown;
    delayMs: number;
  },
): Promise<void> {
  const message =
    input.error instanceof Error ? input.error.message : String(input.error);

  await pool.query(
    `
      UPDATE kafka_outbox
      SET
        status = 'pending',
        attempt_count = attempt_count + 1,

        next_attempt_at =
          NOW() +
          (
            $2::INTEGER *
            INTERVAL '1 millisecond'
          ),

        locked_at = NULL,
        locked_by = NULL,
        last_error = LEFT($3, 2000)
      WHERE id = $1
    `,
    [input.eventId, input.delayMs, message],
  );
}
