INSERT INTO tenants (
  id,
  slug,
  name
)
VALUES (
  '00000000-0000-4000-8000-000000000001',
  'demo-store',
  'OrderFlow Demo Store'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO products (
  id,
  tenant_id,
  sku,
  name,
  price_minor,
  currency
)
VALUES
  (
    '10000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000001',
    'KEYBOARD-001',
    'Mechanical Keyboard',
    1299,
    'GBP'
  ),
  (
    '10000000-0000-4000-8000-000000000002',
    '00000000-0000-4000-8000-000000000001',
    'MOUSE-001',
    'Wireless Mouse',
    2599,
    'GBP'
  )
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory (
  product_id,
  tenant_id,
  available_quantity
)
VALUES
  (
    '10000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000001',
    10
  ),
  (
    '10000000-0000-4000-8000-000000000002',
    '00000000-0000-4000-8000-000000000001',
    5
  )
ON CONFLICT (product_id) DO NOTHING;