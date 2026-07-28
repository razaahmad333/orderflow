import type { Pool, PoolClient } from "pg";

export interface BackgroundJobOutboxEvent {
  id: string;
  queueName: string;
  jobType: string;
  payload: unknown;
  attemptCount: number;
}

interface BackgroundJobOutboxRow {
  id: string;
  queue_name: string;
  job_type: string;
  payload: unknown;
  attempt_count: number;
}

export async function enqueueBackgroundJob(
  client: PoolClient,
  input: {
    tenantId: string;
    queueName: string;
    jobType: string;
    payload: unknown;
  },
): Promise<string> {
  const result = await client.query<{
    id: string;
  }>(
    `
      INSERT INTO background_job_outbox (
        tenant_id,
        queue_name,
        job_type,
        payload
      )
      VALUES ($1, $2, $3, $4::JSONB)
      RETURNING id
    `,
    [
      input.tenantId,
      input.queueName,
      input.jobType,
      JSON.stringify(input.payload),
    ],
  );

  const event = result.rows[0];

  if (!event) {
    throw new Error("Background job outbox event was not returned");
  }

  return event.id;
}

export async function claimBackgroundJobs(
  pool: Pool,
  input: {
    workerId: string;
    batchSize: number;
    lockMs: number;
  },
): Promise<BackgroundJobOutboxEvent[]> {
  const result = await pool.query<BackgroundJobOutboxRow>(
    `
        WITH candidates AS (
          SELECT id
          FROM background_job_outbox
          WHERE enqueued_at IS NULL
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

        UPDATE background_job_outbox AS outbox
        SET
          status = 'processing',
          locked_at = NOW(),
          locked_by = $3
        FROM candidates
        WHERE outbox.id = candidates.id
        RETURNING
          outbox.id,
          outbox.queue_name,
          outbox.job_type,
          outbox.payload,
          outbox.attempt_count
      `,
    [input.lockMs, input.batchSize, input.workerId],
  );

  return result.rows.map((row) => ({
    id: row.id,
    queueName: row.queue_name,
    jobType: row.job_type,
    payload: row.payload,
    attemptCount: row.attempt_count,
  }));
}

export async function markBackgroundJobEnqueued(
  pool: Pool,
  eventId: string,
): Promise<void> {
  await pool.query(
    `
      UPDATE background_job_outbox
      SET
        status = 'enqueued',
        enqueued_at = NOW(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL
      WHERE id = $1
    `,
    [eventId],
  );
}

export async function rescheduleBackgroundJob(
  pool: Pool,
  input: {
    eventId: string;
    error: unknown;
    delayMs: number;
  },
): Promise<void> {
  const errorMessage =
    input.error instanceof Error ? input.error.message : String(input.error);

  await pool.query(
    `
      UPDATE background_job_outbox
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
    [input.eventId, input.delayMs, errorMessage],
  );
}
