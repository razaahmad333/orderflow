BEGIN;

DELETE FROM order_items;
DELETE FROM orders;

UPDATE inventory
SET
  available_quantity = CASE product_id
    WHEN '10000000-0000-4000-8000-000000000001'
      THEN 10
    WHEN '10000000-0000-4000-8000-000000000002'
      THEN 5
    ELSE available_quantity
  END,
  version = 0,
  updated_at = NOW();

COMMIT;