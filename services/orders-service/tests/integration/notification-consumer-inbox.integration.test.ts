import type { Pool } from "pg";
import pino from "pino";

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import {
  notificationConsumerName,
  processOrderCreatedEvent,
} from "../../src/consumers/notification-consumer-inbox";
import { createOrderCreatedEvent } from "../../src/events/order-created-event";
import {
  claimBackgroundJobs,
  markBackgroundJobEnqueued,
} from "../../src/jobs/background-job-outbox";
import type { OrderConfirmedJob } from "../../src/jobs/bullmq";
import { processOrderConfirmedNotification } from "../../src/notifications/order-confirmed-processor";
import { placeOrder } from "../../src/orders/order-service";
import {
  createTestPool,
  keyboardProductId,
  migrateTestDatabase,
  resetTestDatabase,
  testTenantId,
} from "./test-database";

let pool: Pool;
let orderId: string;

const logger = pino({
  level: "silent",
});

beforeAll(async () => {
  await migrateTestDatabase();
  pool = createTestPool();
});

beforeEach(async () => {
  await resetTestDatabase(pool);

  const result = await pool.query<{
    id: string;
  }>(
    `
      INSERT INTO orders (
        tenant_id,
        external_id,
        status,
        total_minor,
        currency
      )
      VALUES (
        $1,
        'consumer-order',
        'confirmed',
        1299,
        'GBP'
      )
      RETURNING id
    `,
    [testTenantId],
  );

  orderId = result.rows[0]!.id;
});

afterAll(async () => {
  await pool.end();
});

function buildEvent(overrides: Partial<ReturnType<typeof createOrderCreatedEvent>> = {}) {
  return {
    ...createOrderCreatedEvent({
      tenantId: testTenantId,
      orderId,
      externalId: "consumer-order",
      totalMinor: "1299",
      currency: "GBP",
      items: [
        {
          productId: keyboardProductId,
          quantity: 1,
        },
      ],
    }),
    ...overrides,
  };
}

const orderOptions = {
  maxTransactionAttempts: 3,
  transactionRetryBaseDelayMs: 10,
  transactionLockTimeoutMs: 2000,
  transactionStatementTimeoutMs: 5000,
  orderCreatedTopic: "order.created.v1",
};

async function runInTransaction<T>(
  callback: Parameters<typeof processOrderCreatedEvent>[0] extends infer Input
    ? (client: NonNullable<Input extends { client: infer Client } ? Client : never>) => Promise<T>
    : never,
): Promise<T> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function countInboxRows(): Promise<number> {
  const result = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*) AS count
      FROM consumer_inbox
      WHERE consumer_name = $1
    `,
    [notificationConsumerName],
  );

  return Number(result.rows[0]?.count ?? 0);
}

async function countNotificationJobs(targetOrderId: string = orderId): Promise<number> {
  const result = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*) AS count
      FROM background_job_outbox
      WHERE queue_name = 'notifications'
        AND job_type = 'order-confirmed'
        AND payload ->> 'orderId' = $1
    `,
    [targetOrderId],
  );

  return Number(result.rows[0]?.count ?? 0);
}

describe("notification consumer inbox processor", () => {
  it("stores the inbox row and background job on first delivery", async () => {
    const event = buildEvent();

    const result = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "10",
        event,
      }),
    );

    expect(result).toBe("processed");
    expect(await countInboxRows()).toBe(1);
    expect(await countNotificationJobs()).toBe(1);
  });

  it("does not create a second job for a duplicate event id", async () => {
    const event = buildEvent();

    await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "10",
        event,
      }),
    );

    const duplicate = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "11",
        event,
      }),
    );

    expect(duplicate).toBe("duplicate");
    expect(await countInboxRows()).toBe(1);
    expect(await countNotificationJobs()).toBe(1);
  });

  it("does not create a second job for a duplicate Kafka position", async () => {
    await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 3,
        offset: "42",
        event: buildEvent(),
      }),
    );

    const duplicate = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 3,
        offset: "42",
        event: buildEvent(),
      }),
    );

    expect(duplicate).toBe("duplicate");
    expect(await countInboxRows()).toBe(1);
    expect(await countNotificationJobs()).toBe(1);
  });

  it("rolls back both inbox and outbox when job insertion fails", async () => {
    const missingTenantId = "00000000-0000-4000-8000-000000000099";

    await expect(
      runInTransaction((client) =>
        processOrderCreatedEvent({
          client,
          consumerName: notificationConsumerName,
          topic: "order.created.v1",
          partition: 0,
          offset: "10",
          event: buildEvent({
            tenantId: missingTenantId,
          }),
        }),
      ),
    ).rejects.toThrow();

    expect(await countInboxRows()).toBe(0);
    expect(await countNotificationJobs()).toBe(0);
  });

  it("treats redelivery after commit as a duplicate without another job", async () => {
    const event = buildEvent();

    const first = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "10",
        event,
      }),
    );

    const redelivery = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "10",
        event,
      }),
    );

    expect(first).toBe("processed");
    expect(redelivery).toBe("duplicate");
    expect(await countInboxRows()).toBe(1);
    expect(await countNotificationJobs()).toBe(1);
  });

  it("creates the notification job only after Kafka consumption and reaches one provider effect", async () => {
    const placedOrder = await placeOrder(
      pool,
      {
        tenantId: testTenantId,
        externalId: "kafka-only-notification-001",
        currency: "GBP",
        items: [
          {
            productId: keyboardProductId,
            quantity: 1,
          },
        ],
      },
      orderOptions,
    );

    const preConsumptionJobs = await countNotificationJobs(placedOrder.id);
    expect(preConsumptionJobs).toBe(0);

    const outbox = await pool.query<{
      payload: ReturnType<typeof buildEvent>;
    }>(
      `
        SELECT payload
        FROM kafka_outbox
        WHERE aggregate_id = $1
          AND event_type = 'order.created'
      `,
      [placedOrder.id],
    );

    expect(outbox.rows).toHaveLength(1);

    const consumed = await runInTransaction((client) =>
      processOrderCreatedEvent({
        client,
        consumerName: notificationConsumerName,
        topic: "order.created.v1",
        partition: 0,
        offset: "21",
        event: outbox.rows[0]!.payload,
      }),
    );

    expect(consumed).toBe("processed");
    expect(await countInboxRows()).toBe(1);
    expect(await countNotificationJobs(placedOrder.id)).toBe(1);

    const claimed = await claimBackgroundJobs(pool, {
      workerId: "integration-dispatcher",
      batchSize: 10,
      lockMs: 30_000,
    });

    expect(claimed).toHaveLength(1);

    const dispatchedJob = claimed[0]!;

    const delivery = await processOrderConfirmedNotification({
      pool,
      logger,
      jobId: dispatchedJob.id,
      data: dispatchedJob.payload as OrderConfirmedJob,
      simulateCrashAfterProviderSuccess: false,
    });

    expect(delivery.providerCreatedNow).toBe(true);

    await markBackgroundJobEnqueued(pool, dispatchedJob.id);

    const deliveryRows = await pool.query<{
      count: string;
      status: string;
    }>(
      `
        SELECT
          COUNT(*) AS count,
          MAX(status) AS status
        FROM notification_deliveries
        WHERE order_id = $1
      `,
      [placedOrder.id],
    );

    expect(deliveryRows.rows[0]).toEqual({
      count: "1",
      status: "delivered",
    });

    const providerRows = await pool.query<{
      count: string;
    }>(
      `
        SELECT COUNT(*) AS count
        FROM simulated_provider_messages
        WHERE order_id = $1
      `,
      [placedOrder.id],
    );

    expect(providerRows.rows[0]?.count).toBe("1");
  });
});
