import path from "node:path";

import pino from "pino";

import { runMigrations } from "../migrations";

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const migrationsDirectory =
    process.env.MIGRATIONS_DIRECTORY ??
    path.resolve(process.cwd(), "database/migrations");

  const logger = pino({
    name: "orderflow-migrator",
    level: process.env.LOG_LEVEL ?? "info",
  });

  logger.info(
    {
      migrationsDirectory,
    },
    "Starting database migrations",
  );

  await runMigrations({
    databaseUrl,
    migrationsDirectory,
    logger,
  });

  logger.info("Database migrations completed");
}

main().catch((error: unknown) => {
  const logger = pino({
    name: "orderflow-migrator",
  });

  logger.fatal(
    {
      err: error,
    },
    "Database migration failed",
  );

  process.exitCode = 1;
});
