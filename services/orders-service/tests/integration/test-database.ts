import path from "node:path";

import pino from "pino";
import { Pool } from "pg";

import { runMigrations } from "../../src/migrations";

export const testTenantId = "00000000-0000-4000-8000-000000000001";

export const keyboardProductId = "10000000-0000-4000-8000-000000000001";

export const mouseProductId = "10000000-0000-4000-8000-000000000002";

const logger = pino({
  level: "silent",
});

export function createTestPool(): Pool {
  const databaseUrl = process.env.TEST_DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("TEST_DATABASE_URL is required");
  }

  return new Pool({
    connectionString: databaseUrl,
    max: 10,
    connectionTimeoutMillis: 2_000,
    application_name: "orders-service-integration-tests",
  });
}

export async function migrateTestDatabase(): Promise<void> {
  const databaseUrl = process.env.TEST_DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("TEST_DATABASE_URL is required");
  }

  await runMigrations({
    databaseUrl,

    migrationsDirectory: path.resolve(
      __dirname,
      "../../../../database/migrations",
    ),

    logger,
  });
}

export async function resetTestDatabase(pool: Pool): Promise<void> {
  await pool.query(`
    TRUNCATE TABLE
      order_items,
      orders,
      inventory,
      products,
      tenants
    CASCADE
  `);

  await pool.query(
    `
      INSERT INTO tenants (
        id,
        slug,
        name
      )
      VALUES (
        $1,
        'integration-store',
        'Integration Test Store'
      )
    `,
    [testTenantId],
  );

  await pool.query(
    `
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
          $1,
          $3,
          'KEYBOARD-001',
          'Mechanical Keyboard',
          1299,
          'GBP'
        ),
        (
          $2,
          $3,
          'MOUSE-001',
          'Wireless Mouse',
          2599,
          'GBP'
        )
    `,
    [keyboardProductId, mouseProductId, testTenantId],
  );

  await pool.query(
    `
      INSERT INTO inventory (
        product_id,
        tenant_id,
        available_quantity
      )
      VALUES
        ($1, $3, 10),
        ($2, $3, 5)
    `,
    [keyboardProductId, mouseProductId, testTenantId],
  );
}
