DROP INDEX IF EXISTS orders_tenant_created_at_idx;

CREATE INDEX orders_tenant_created_at_id_idx
  ON orders (
    tenant_id,
    created_at DESC,
    id DESC
  );
