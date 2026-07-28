import type { Pool } from "pg";

interface ProviderMessageRow {
  message_id: string;
  created_now: boolean;
}

export interface ProviderDeliveryResult {
  messageId: string;

  /*
   * true:
   *   the provider created a new delivery.
   *
   * false:
   *   the same idempotency key was previously accepted.
   */
  createdNow: boolean;
}

export async function sendOrderConfirmedNotification(
  pool: Pool,
  input: {
    idempotencyKey: string;
    tenantId: string;
    orderId: string;
    externalId: string;
    totalMinor: string;
    currency: string;
  },
): Promise<ProviderDeliveryResult> {
  const result = await pool.query<ProviderMessageRow>(
    `
        WITH inserted AS (
          INSERT INTO simulated_provider_messages (
            idempotency_key,
            tenant_id,
            order_id,
            payload
          )
          VALUES (
            $1,
            $2,
            $3,
            $4::JSONB
          )
          ON CONFLICT (idempotency_key)
          DO NOTHING

          RETURNING
            message_id,
            TRUE AS created_now
        )

        SELECT *
        FROM inserted

        UNION ALL

        SELECT
          message_id,
          FALSE AS created_now
        FROM simulated_provider_messages
        WHERE idempotency_key = $1
          AND NOT EXISTS (
            SELECT 1
            FROM inserted
          )

        LIMIT 1
      `,
    [
      input.idempotencyKey,
      input.tenantId,
      input.orderId,

      JSON.stringify({
        externalId: input.externalId,
        totalMinor: input.totalMinor,
        currency: input.currency,
      }),
    ],
  );

  const row = result.rows[0];

  if (!row) {
    throw new Error("Notification provider returned no result");
  }

  return {
    messageId: row.message_id,
    createdNow: row.created_now,
  };
}
