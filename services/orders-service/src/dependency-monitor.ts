import type { Pool } from "pg";
import type { Logger } from "pino";

import type { ReadinessState } from "./app";

interface StartDatabaseMonitorOptions {
  pool: Pool;
  logger: Logger;
  readiness: ReadinessState;
  intervalMs: number;
}

export interface DependencyMonitor {
  checkNow(): Promise<void>;
  stop(): void;
}

export function startDatabaseMonitor({
  pool,
  logger,
  readiness,
  intervalMs
}: StartDatabaseMonitorOptions): DependencyMonitor {
  let stopped = false;
  let checkInProgress = false;
  let timer: NodeJS.Timeout | undefined;
  let previousState: boolean | undefined;

  async function checkNow(): Promise<void> {
    if (stopped || checkInProgress) {
      return;
    }

    checkInProgress = true;

    try {
      await pool.query("SELECT 1");

      readiness.ready = true;
      delete readiness.reason;

      if (previousState !== true) {
        logger.info("PostgreSQL dependency is ready");
      }

      previousState = true;
    } catch (error) {
      readiness.ready = false;
      readiness.reason = "database_unavailable";

      if (previousState !== false) {
        logger.error(
          { err: error },
          "PostgreSQL dependency is unavailable"
        );
      }

      previousState = false;
    } finally {
      checkInProgress = false;
    }
  }

  async function scheduleNextCheck(): Promise<void> {
    await checkNow();

    if (stopped) {
      return;
    }

    timer = setTimeout(() => {
      void scheduleNextCheck();
    }, intervalMs);

    timer.unref();
  }

  void scheduleNextCheck();

  return {
    checkNow,

    stop(): void {
      stopped = true;

      if (timer) {
        clearTimeout(timer);
      }
    }
  };
}
