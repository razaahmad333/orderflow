import pino, { type Logger } from "pino";

import type { AppConfig } from "./config";

export function createLogger(config: AppConfig): Logger {
  return pino({
    level: config.LOG_LEVEL,

    base: {
      service: config.SERVICE_NAME,
      environment: config.NODE_ENV
    },

    timestamp: pino.stdTimeFunctions.isoTime
  });
}
