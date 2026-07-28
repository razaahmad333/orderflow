CREATE TABLE cache_invalidation_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id UUID NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  cache_key TEXT NOT NULL,

  status TEXT NOT NULL DEFAULT 'pending',

  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  locked_at TIMESTAMPTZ,
  locked_by TEXT,

  last_error TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,

  CONSTRAINT cache_invalidation_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE CASCADE,

  CONSTRAINT cache_invalidation_status_valid
    CHECK (
      status IN (
        'pending',
        'processing',
        'processed'
      )
    ),

  CONSTRAINT cache_invalidation_attempts_non_negative
    CHECK (attempt_count >= 0),

  CONSTRAINT cache_invalidation_entity_type_not_blank
    CHECK (length(trim(entity_type)) > 0),

  CONSTRAINT cache_invalidation_key_not_blank
    CHECK (length(trim(cache_key)) > 0)
);

CREATE INDEX cache_invalidation_pending_idx
  ON cache_invalidation_outbox (
    next_attempt_at,
    created_at
  )
  WHERE processed_at IS NULL;