import type { Pool, PoolClient } from "pg";

export interface CacheInvalidationEvent {
  id: string;
  cacheKey: string;
  attemptCount: number;
}

interface CacheInvalidationRow {
  id: string;
  cache_key: string;
  attempt_count: number;
}

export async function enqueueCacheInvalidation(
  client: PoolClient,
  input: {
    tenantId: string;
    entityType: string;
    entityId: string;
    cacheKey: string;
  },
): Promise<string> {
  const result = await client.query<{
    id: string;
  }>(
    `
      INSERT INTO cache_invalidation_outbox (
        tenant_id,
        entity_type,
        entity_id,
        cache_key
      )
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `,
    [input.tenantId, input.entityType, input.entityId, input.cacheKey],
  );

  const row = result.rows[0];

  if (!row) {
    throw new Error("Cache invalidation event was not returned");
  }

  return row.id;
}

export async function claimCacheInvalidations(
  pool: Pool,
  input: {
    workerId: string;
    batchSize: number;
    lockMs: number;
  },
): Promise<CacheInvalidationEvent[]> {
  const result = await pool.query<CacheInvalidationRow>(
    `
        WITH candidates AS (
          SELECT id
          FROM cache_invalidation_outbox
          WHERE processed_at IS NULL
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

        UPDATE cache_invalidation_outbox AS outbox
        SET
          status = 'processing',
          locked_at = NOW(),
          locked_by = $3
        FROM candidates
        WHERE outbox.id = candidates.id
        RETURNING
          outbox.id,
          outbox.cache_key,
          outbox.attempt_count
      `,
    [input.lockMs, input.batchSize, input.workerId],
  );

  return result.rows.map((row) => ({
    id: row.id,
    cacheKey: row.cache_key,
    attemptCount: row.attempt_count,
  }));
}

export async function markCacheInvalidationProcessed(
  pool: Pool,
  eventId: string,
): Promise<void> {
  await pool.query(
    `
      UPDATE cache_invalidation_outbox
      SET
        status = 'processed',
        processed_at = NOW(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL
      WHERE id = $1
    `,
    [eventId],
  );
}

export async function rescheduleCacheInvalidation(
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
      UPDATE cache_invalidation_outbox
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
