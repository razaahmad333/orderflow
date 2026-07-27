BEGIN;

DELETE FROM orders
WHERE external_id LIKE 'query-performance-%';

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

  'query-performance-' ||
    LPAD(sequence_number::TEXT, 7, '0'),

  CASE
    WHEN sequence_number % 5 = 0
      THEN 'cancelled'
    ELSE 'confirmed'
  END,

  100 +
    (
      sequence_number * 7919
    ) % 500000,

  'GBP',
  NULL,

  NOW() -
    (
      sequence_number ||
      ' milliseconds'
    )::INTERVAL,

  NOW() -
    (
      sequence_number ||
      ' milliseconds'
    )::INTERVAL

FROM generate_series(
  1,
  100000
) AS sequence_number;

COMMIT;

VACUUM (ANALYZE) orders;