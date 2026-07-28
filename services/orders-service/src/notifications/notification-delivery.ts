import type { Pool } from "pg";

export type NotificationDeliveryStatus = "pending" | "delivered";

export interface NotificationDelivery {
  idempotencyKey: string;
  status: NotificationDeliveryStatus;
  attemptCount: number;
  providerMessageId: string | null;
}

interface NotificationDeliveryRow {
  idempotency_key: string;
  status: NotificationDeliveryStatus;
  attempt_count: number;
  provider_message_id: string | null;
}

export function createOrderNotificationKey(
  tenantId: string,
  orderId: string,
): string {
  return ["notification", "order-confirmed", "v1", tenantId, orderId].join(":");
}

export async function reserveNotificationDelivery(
  pool: Pool,
  input: {
    idempotencyKey: string;
    tenantId: string;
    orderId: string;
    sourceJobId: string;
  },
): Promise<NotificationDelivery> {
  const result = await pool.query<NotificationDeliveryRow>(
    `
        WITH inserted AS (
          INSERT INTO notification_deliveries (
            idempotency_key,
            tenant_id,
            order_id,
            source_job_id,
            notification_type
          )
          VALUES (
            $1,
            $2,
            $3,
            $4,
            'order-confirmed'
          )
          ON CONFLICT (idempotency_key)
          DO NOTHING

          RETURNING
            idempotency_key,
            status,
            attempt_count,
            provider_message_id
        )

        SELECT *
        FROM inserted

        UNION ALL

        SELECT
          idempotency_key,
          status,
          attempt_count,
          provider_message_id
        FROM notification_deliveries
        WHERE idempotency_key = $1
          AND NOT EXISTS (
            SELECT 1
            FROM inserted
          )

        LIMIT 1
      `,
    [input.idempotencyKey, input.tenantId, input.orderId, input.sourceJobId],
  );

  const row = result.rows[0];

  if (!row) {
    throw new Error("Notification delivery could not be reserved");
  }

  return {
    idempotencyKey: row.idempotency_key,
    status: row.status,
    attemptCount: row.attempt_count,
    providerMessageId: row.provider_message_id,
  };
}

export async function recordNotificationAttempt(
  pool: Pool,
  idempotencyKey: string,
): Promise<void> {
  await pool.query(
    `
      UPDATE notification_deliveries
      SET
        attempt_count = attempt_count + 1,
        last_attempt_at = NOW(),
        last_error = NULL
      WHERE idempotency_key = $1
    `,
    [idempotencyKey],
  );
}

export async function recordNotificationFailure(
  pool: Pool,
  input: {
    idempotencyKey: string;
    error: unknown;
  },
): Promise<void> {
  const message =
    input.error instanceof Error ? input.error.message : String(input.error);

  await pool.query(
    `
      UPDATE notification_deliveries
      SET last_error = LEFT($2, 2000)
      WHERE idempotency_key = $1
    `,
    [input.idempotencyKey, message],
  );
}

export async function markNotificationDelivered(
  pool: Pool,
  input: {
    idempotencyKey: string;
    providerMessageId: string;
  },
): Promise<void> {
  await pool.query(
    `
      UPDATE notification_deliveries
      SET
        status = 'delivered',
        provider_message_id = $2,
        delivered_at =
          COALESCE(
            delivered_at,
            NOW()
          ),
        last_error = NULL
      WHERE idempotency_key = $1
    `,
    [input.idempotencyKey, input.providerMessageId],
  );
}
