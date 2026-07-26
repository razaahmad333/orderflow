ALTER TABLE orders
  ADD COLUMN request_fingerprint CHAR(64);

ALTER TABLE orders
  ADD CONSTRAINT orders_request_fingerprint_format
  CHECK (
    request_fingerprint IS NULL
    OR request_fingerprint ~ '^[a-f0-9]{64}$'
  );