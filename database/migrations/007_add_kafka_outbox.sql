CREATE TABLE kafka_outbox (
  id UUID PRIMARY KEY,

  aggregate_type TEXT NOT NULL,
  aggregate_id UUID NOT NULL,

  event_type TEXT NOT NULL,
  event_version INTEGER NOT NULL,

  topic TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  payload JSONB NOT NULL,

  status TEXT NOT NULL DEFAULT 'pending',

  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  locked_at TIMESTAMPTZ,
  locked_by TEXT,

  last_error TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,

  CONSTRAINT kafka_outbox_status_valid
    CHECK (
      status IN (
        'pending',
        'processing',
        'published'
      )
    ),

  CONSTRAINT kafka_outbox_attempts_non_negative
    CHECK (attempt_count >= 0),

  CONSTRAINT kafka_outbox_event_version_positive
    CHECK (event_version > 0)
);

CREATE INDEX kafka_outbox_pending_idx
  ON kafka_outbox (
    next_attempt_at,
    created_at
  )
  WHERE published_at IS NULL;