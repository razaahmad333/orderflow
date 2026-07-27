import "dotenv/config";

import { createApp, type ReadinessState } from "./app";
import { loadConfig } from "./config";
import { createDatabasePool } from "./database";
import { startDatabaseMonitor } from "./dependency-monitor";
import { createLogger } from "./logger";
import { createRedisClient } from "./redis";
import { SingleFlight } from "./single-flight";
import { RedisDistributedLock } from "./distributed-lock";

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config);

  const readiness: ReadinessState = {
    ready: false,
    reason: "startup_check_pending",
  };

  const pool = createDatabasePool(config, logger);
  const redis = createRedisClient(config, logger);

  const productDistributedLock = new RedisDistributedLock(redis);

  const productSingleFlight = new SingleFlight();

  void redis.connect().catch((error) => {
    logger.warn(
      { err: error },
      "Initial Redis connection failed; cache bypass remains available",
    );
  });

  const app = createApp({
    config,
    logger,
    readiness,
    pool,
    redis,
    productSingleFlight,
    productDistributedLock,
  });
  const databaseMonitor = startDatabaseMonitor({
    pool,
    logger,
    readiness,
    intervalMs: config.DB_HEALTHCHECK_INTERVAL_MS,
  });

  const host = "0.0.0.0";

  const server = app.listen(config.PORT, host, () => {
    logger.info(
      {
        host,
        port: config.PORT,
      },
      "HTTP server started",
    );
  });

  let shutdownStarted = false;

  async function shutdown(signal: string, exitCode = 0): Promise<void> {
    if (shutdownStarted) {
      logger.warn({ signal }, "Shutdown already in progress");
      return;
    }

    shutdownStarted = true;

    readiness.ready = false;
    readiness.reason = "service_shutting_down";

    databaseMonitor.stop();

    logger.warn(
      {
        signal,
        timeoutMs: config.SHUTDOWN_TIMEOUT_MS,
      },
      "Graceful shutdown started",
    );

    const forcedShutdownTimer = setTimeout(() => {
      logger.error("Graceful shutdown deadline exceeded");

      server.closeAllConnections();
      process.exit(1);
    }, config.SHUTDOWN_TIMEOUT_MS);

    forcedShutdownTimer.unref();

    try {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      });

      logger.info("HTTP server stopped accepting connections");

      await pool.end();

      logger.info("PostgreSQL pool closed");

      if (redis.isOpen) {
        await redis.close();

        logger.info("Redis connection closed");
      }

      logger.info("Graceful shutdown completed");

      process.exitCode = exitCode;
    } catch (error) {
      logger.error({ err: error }, "Graceful shutdown failed");

      process.exitCode = 1;
    } finally {
      clearTimeout(forcedShutdownTimer);
      logger.flush();
    }
  }

  server.on("error", (error) => {
    logger.fatal({ err: error }, "HTTP server failed");
    void shutdown("server_error", 1);
  });

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.on("uncaughtException", (error) => {
    logger.fatal({ err: error }, "Uncaught exception");
    void shutdown("uncaughtException", 1);
  });

  process.on("unhandledRejection", (reason) => {
    logger.fatal({ reason }, "Unhandled promise rejection");
    void shutdown("unhandledRejection", 1);
  });
}

void main().catch((error: unknown) => {
  const message =
    error instanceof Error ? (error.stack ?? error.message) : String(error);

  process.stderr.write(`Fatal bootstrap error: ${message}\n`);
  process.exit(1);
});
