CREATE TABLE background_job_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id UUID NOT NULL,

  queue_name TEXT NOT NULL,
  job_type TEXT NOT NULL,
  payload JSONB NOT NULL,

  status TEXT NOT NULL DEFAULT 'pending',

  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  locked_at TIMESTAMPTZ,
  locked_by TEXT,

  last_error TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  enqueued_at TIMESTAMPTZ,

  CONSTRAINT background_job_outbox_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE CASCADE,

  CONSTRAINT background_job_outbox_status_valid
    CHECK (
      status IN (
        'pending',
        'processing',
        'enqueued'
      )
    ),

  CONSTRAINT background_job_outbox_attempts_non_negative
    CHECK (attempt_count >= 0),

  CONSTRAINT background_job_outbox_queue_not_blank
    CHECK (length(trim(queue_name)) > 0),

  CONSTRAINT background_job_outbox_type_not_blank
    CHECK (length(trim(job_type)) > 0)
);

CREATE INDEX background_job_outbox_pending_idx
  ON background_job_outbox (
    next_attempt_at,
    created_at
  )
  WHERE enqueued_at IS NULL;