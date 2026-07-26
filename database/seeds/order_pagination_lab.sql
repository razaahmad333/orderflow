BEGIN;

DELETE FROM orders
WHERE external_id LIKE 'pagination-lab-%';

INSERT INTO orders (
  tenant_id,
  external_id,
  status,
  total_minor,
  currency,
  request_fingerprint,
  created_at,
  updated_at
)
SELECT
  '00000000-0000-4000-8000-000000000001',
  'pagination-lab-' ||
    LPAD(sequence_number::TEXT, 6, '0'),

  CASE
    WHEN sequence_number % 10 = 0
      THEN 'cancelled'
    ELSE 'confirmed'
  END,

  1299,
  'GBP',
  NULL,

  NOW() -
    (
      sequence_number ||
      ' seconds'
    )::INTERVAL,

  NOW() -
    (
      sequence_number ||
      ' seconds'
    )::INTERVAL
FROM generate_series(
  1,
  50000
) AS sequence_number;

COMMIT;

ANALYZE orders;