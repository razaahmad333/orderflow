CREATE INDEX orders_tenant_status_total_id_idx
  ON orders (
    tenant_id,
    status,
    total_minor DESC,
    id DESC
  )
  INCLUDE (
    external_id,
    currency,
    created_at
  );