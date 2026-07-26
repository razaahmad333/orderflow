import "dotenv/config";

import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import pino from "pino";
import { Client } from "pg";

const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: {
    service: "orderflow-migrator"
  },
  timestamp: pino.stdTimeFunctions.isoTime
});

const migrationFilePattern = /^\d{3}_[a-z0-9_]+\.sql$/;

function calculateChecksum(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}

async function runMigrations(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const migrationsDirectory = path.resolve(
    __dirname,
    "../../../../database/migrations"
  );

  const client = new Client({
    connectionString: databaseUrl,
    application_name: "orderflow-migrator",
    connectionTimeoutMillis: 5_000
  });

  await client.connect();

  let advisoryLockAcquired = false;

  try {
    await client.query(
      "SELECT pg_advisory_lock(hashtext($1))",
      ["orderflow_schema_migrations"]
    );

    advisoryLockAcquired = true;

    logger.info("Migration advisory lock acquired");

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

    if (filenames.length === 0) {
      logger.warn(
        { migrationsDirectory },
        "No migration files found"
      );

      return;
    }

    for (const filename of filenames) {
      const filePath = path.join(migrationsDirectory, filename);
      const sql = await readFile(filePath, "utf8");
      const checksum = calculateChecksum(sql);

      const existingMigration = await client.query<{
        checksum: string;
      }>(
        `
          SELECT checksum
          FROM schema_migrations
          WHERE filename = $1
        `,
        [filename]
      );

      if (existingMigration.rowCount === 1) {
        const recordedChecksum =
          existingMigration.rows[0]?.checksum.trim();

        if (recordedChecksum !== checksum) {
          throw new Error(
            `Applied migration ${filename} has been modified`
          );
        }

        logger.info(
          { filename },
          "Migration already applied"
        );

        continue;
      }

      logger.info(
        { filename },
        "Applying migration"
      );

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
          [filename, checksum]
        );

        await client.query("COMMIT");

        logger.info(
          { filename },
          "Migration applied successfully"
        );
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    if (advisoryLockAcquired) {
      await client.query(
        "SELECT pg_advisory_unlock(hashtext($1))",
        ["orderflow_schema_migrations"]
      );

      logger.info("Migration advisory lock released");
    }

    await client.end();
  }
}

runMigrations().catch((error: unknown) => {
  logger.fatal(
    {
      err: error
    },
    "Migration execution failed"
  );

  process.exitCode = 1;
});
