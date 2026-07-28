import type { PoolClient } from "pg";

import { enqueueBackgroundJob } from "../jobs/background-job-outbox";
import {
  notificationQueueName,
  type OrderConfirmedJob,
} from "../jobs/bullmq";
import type { OrderCreatedEvent } from "../events/order-created-event";

export const notificationConsumerName = "notification-service-v1";

export type OrderCreatedProcessingResult = "processed" | "duplicate";

interface ProcessOrderCreatedEventInput {
  client: PoolClient;
  consumerName: string;
  topic: string;
  partition: number;
  offset: string;
  event: OrderCreatedEvent;
}

function createStoredEventType(event: OrderCreatedEvent): string {
  return `${event.eventType}.v${event.eventVersion}`;
}

function createOrderConfirmedJob(event: OrderCreatedEvent): OrderConfirmedJob {
  return {
    tenantId: event.tenantId,
    orderId: event.orderId,
    externalId: event.externalId,
    totalMinor: event.totalMinor,
    currency: event.currency,
  };
}

export async function processOrderCreatedEvent({
  client,
  consumerName,
  topic,
  partition,
  offset,
  event,
}: ProcessOrderCreatedEventInput): Promise<OrderCreatedProcessingResult> {
  const inboxInsert = await client.query<{
    event_id: string;
  }>(
    `
      INSERT INTO consumer_inbox (
        consumer_name,
        event_id,
        topic,
        partition_number,
        message_offset,
        event_type,
        payload
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::JSONB)
      ON CONFLICT DO NOTHING
      RETURNING event_id
    `,
    [
      consumerName,
      event.eventId,
      topic,
      partition,
      offset,
      createStoredEventType(event),
      JSON.stringify(event),
    ],
  );

  if (inboxInsert.rowCount === 0) {
    return "duplicate";
  }

  await enqueueBackgroundJob(client, {
    tenantId: event.tenantId,
    queueName: notificationQueueName,
    jobType: "order-confirmed",
    payload: createOrderConfirmedJob(event),
  });

  return "processed";
}
