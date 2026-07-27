ALTER TABLE products
  ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE products
  ADD CONSTRAINT products_version_non_negative
  CHECK (version >= 0);