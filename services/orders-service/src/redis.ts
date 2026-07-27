import { createClient, type RedisClientType } from "redis";
import type { Logger } from "pino";

import type { AppConfig } from "./config";

export function createRedisClient(
  config: AppConfig,
  logger: Logger,
): RedisClientType {
  const client = createClient({
    url: config.REDIS_URL,

    /*
     * Reject commands immediately while disconnected.
     * The application can then fall back to PostgreSQL.
     */
    disableOfflineQueue: true,

    socket: {
      connectTimeout: config.REDIS_CONNECT_TIMEOUT_MS,
    },
  });

  client.on("ready", () => {
    logger.info("Redis connection ready");
  });

  client.on("reconnecting", () => {
    logger.warn("Redis reconnecting");
  });

  client.on("error", (error) => {
    logger.warn({ err: error }, "Redis client error");
  });

  return client;
}
