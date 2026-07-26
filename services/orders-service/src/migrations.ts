import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import { Client } from "pg";
import type { Logger } from "pino";

interface RunMigrationsOptions {
  databaseUrl: string;
  migrationsDirectory: string;
  logger: Logger;
}

const migrationFilePattern = /^\d{3}_[a-z0-9_]+\.sql$/;

function calculateChecksum(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}

export async function runMigrations({
  databaseUrl,
  migrationsDirectory,
  logger,
}: RunMigrationsOptions): Promise<void> {
  const client = new Client({
    connectionString: databaseUrl,
    application_name: "orderflow-migrator",
    connectionTimeoutMillis: 5_000,
  });

  await client.connect();

  let advisoryLockAcquired = false;

  try {
    await client.query("SELECT pg_advisory_lock(hashtext($1))", [
      "orderflow_schema_migrations",
    ]);

    advisoryLockAcquired = true;

    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        checksum CHAR(64) NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const filenames = (await readdir(migrationsDirectory))
      .filter((filename) => migrationFilePattern.test(filename))
      .sort();

    for (const filename of filenames) {
      const filePath = path.join(migrationsDirectory, filename);

      const sql = await readFile(filePath, "utf8");

      const checksum = calculateChecksum(sql);

      const existing = await client.query<{
        checksum: string;
      }>(
        `
            SELECT checksum
            FROM schema_migrations
            WHERE filename = $1
          `,
        [filename],
      );

      if (existing.rowCount === 1) {
        const recordedChecksum = existing.rows[0]?.checksum.trim();

        if (recordedChecksum !== checksum) {
          throw new Error(`Applied migration ${filename} has been modified`);
        }

        logger.debug({ filename }, "Migration already applied");

        continue;
      }

      logger.info({ filename }, "Applying migration");

      await client.query("BEGIN");

      try {
        await client.query(sql);

        await client.query(
          `
            INSERT INTO schema_migrations (
              filename,
              checksum
            )
            VALUES ($1, $2)
          `,
          [filename, checksum],
        );

        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    if (advisoryLockAcquired) {
      await client.query("SELECT pg_advisory_unlock(hashtext($1))", [
        "orderflow_schema_migrations",
      ]);
    }

    await client.end();
  }
}
