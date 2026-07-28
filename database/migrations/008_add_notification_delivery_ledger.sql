CREATE TABLE notification_deliveries (
  idempotency_key TEXT PRIMARY KEY,

  tenant_id UUID NOT NULL,
  order_id UUID NOT NULL,
  source_job_id TEXT NOT NULL,

  notification_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',

  attempt_count INTEGER NOT NULL DEFAULT 0,

  provider_message_id UUID,
  last_error TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_attempt_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,

  CONSTRAINT notification_delivery_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE CASCADE,

  CONSTRAINT notification_delivery_order_fk
    FOREIGN KEY (order_id)
    REFERENCES orders(id)
    ON DELETE CASCADE,

  CONSTRAINT notification_delivery_status_valid
    CHECK (
      status IN (
        'pending',
        'delivered'
      )
    ),

  CONSTRAINT notification_delivery_attempts_non_negative
    CHECK (attempt_count >= 0)
);

CREATE INDEX notification_deliveries_order_idx
  ON notification_deliveries (
    tenant_id,
    order_id
  );

-- This table simulates an external provider that
-- supports idempotency keys.
CREATE TABLE simulated_provider_messages (
  message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  idempotency_key TEXT NOT NULL UNIQUE,

  tenant_id UUID NOT NULL,
  order_id UUID NOT NULL,

  payload JSONB NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT simulated_provider_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE CASCADE,

  CONSTRAINT simulated_provider_order_fk
    FOREIGN KEY (order_id)
    REFERENCES orders(id)
    ON DELETE CASCADE
);