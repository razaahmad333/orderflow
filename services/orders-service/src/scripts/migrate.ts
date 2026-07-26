import "dotenv/config";

import path from "node:path";

import pino from "pino";

import { runMigrations } from "../migrations";

const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",

  base: {
    service: "orderflow-migrator",
  },

  timestamp: pino.stdTimeFunctions.isoTime,
});

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const migrationsDirectory = path.resolve(
    __dirname,
    "../../../../database/migrations",
  );

  await runMigrations({
    databaseUrl,
    migrationsDirectory,
    logger,
  });
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Migration execution failed");

  process.exitCode = 1;
});
