CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  slug TEXT NOT NULL,
  name TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT tenants_slug_unique
    UNIQUE (slug),

  CONSTRAINT tenants_slug_format
    CHECK (slug ~ '^[a-z0-9]+(?:-[a-z0-9]+)*$')
);

CREATE TABLE products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id UUID NOT NULL,
  sku TEXT NOT NULL,
  name TEXT NOT NULL,

  price_minor BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL,

  active BOOLEAN NOT NULL DEFAULT TRUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT products_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE RESTRICT,

  CONSTRAINT products_tenant_sku_unique
    UNIQUE (tenant_id, sku),

  CONSTRAINT products_id_tenant_unique
    UNIQUE (id, tenant_id),

  CONSTRAINT products_price_non_negative
    CHECK (price_minor >= 0),

  CONSTRAINT products_currency_format
    CHECK (currency ~ '^[A-Z]{3}$'),

  CONSTRAINT products_sku_not_blank
    CHECK (length(trim(sku)) > 0)
);

CREATE TABLE inventory (
  product_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,

  available_quantity INTEGER NOT NULL DEFAULT 0,
  version BIGINT NOT NULL DEFAULT 0,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT inventory_product_tenant_fk
    FOREIGN KEY (product_id, tenant_id)
    REFERENCES products(id, tenant_id)
    ON DELETE CASCADE,

  CONSTRAINT inventory_quantity_non_negative
    CHECK (available_quantity >= 0),

  CONSTRAINT inventory_version_non_negative
    CHECK (version >= 0)
);

CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id UUID NOT NULL,
  external_id TEXT NOT NULL,

  status TEXT NOT NULL DEFAULT 'pending',

  total_minor BIGINT NOT NULL DEFAULT 0,
  currency VARCHAR(3) NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT orders_tenant_fk
    FOREIGN KEY (tenant_id)
    REFERENCES tenants(id)
    ON DELETE RESTRICT,

  CONSTRAINT orders_tenant_external_id_unique
    UNIQUE (tenant_id, external_id),

  CONSTRAINT orders_id_tenant_unique
    UNIQUE (id, tenant_id),

  CONSTRAINT orders_status_valid
    CHECK (
      status IN (
        'pending',
        'confirmed',
        'cancelled',
        'failed'
      )
    ),

  CONSTRAINT orders_total_non_negative
    CHECK (total_minor >= 0),

  CONSTRAINT orders_currency_format
    CHECK (currency ~ '^[A-Z]{3}$'),

  CONSTRAINT orders_external_id_not_blank
    CHECK (length(trim(external_id)) > 0)
);

CREATE TABLE order_items (
  order_id UUID NOT NULL,
  product_id UUID NOT NULL,
  tenant_id UUID NOT NULL,

  quantity INTEGER NOT NULL,
  unit_price_minor BIGINT NOT NULL,

  line_total_minor BIGINT
    GENERATED ALWAYS AS (
      quantity::BIGINT * unit_price_minor
    ) STORED,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT order_items_pk
    PRIMARY KEY (order_id, product_id),

  CONSTRAINT order_items_order_tenant_fk
    FOREIGN KEY (order_id, tenant_id)
    REFERENCES orders(id, tenant_id)
    ON DELETE CASCADE,

  CONSTRAINT order_items_product_tenant_fk
    FOREIGN KEY (product_id, tenant_id)
    REFERENCES products(id, tenant_id)
    ON DELETE RESTRICT,

  CONSTRAINT order_items_quantity_positive
    CHECK (quantity > 0),

  CONSTRAINT order_items_unit_price_non_negative
    CHECK (unit_price_minor >= 0)
);

CREATE INDEX products_tenant_active_idx
  ON products (tenant_id, active);

CREATE INDEX orders_tenant_created_at_idx
  ON orders (tenant_id, created_at DESC);

CREATE INDEX orders_tenant_status_created_at_idx
  ON orders (tenant_id, status, created_at DESC);

CREATE INDEX order_items_product_id_idx
  ON order_items (product_id);