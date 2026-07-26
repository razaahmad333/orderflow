import "dotenv/config";

import pino from "pino";
import {
  Pool,
  type PoolClient
} from "pg";

type LabMode =
  | "unbounded"
  | "bounded";

interface TaskResult {
  taskId: number;
  succeeded: boolean;
  waitMs: number;
}

const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",

  base: {
    lab: "pool-saturation"
  },

  timestamp: pino.stdTimeFunctions.isoTime
});

function readPositiveInteger(
  name: string,
  defaultValue: number
): number {
  const rawValue = process.env[name];

  if (rawValue === undefined) {
    return defaultValue;
  }

  const parsedValue = Number(rawValue);

  if (
    !Number.isInteger(parsedValue) ||
    parsedValue <= 0
  ) {
    throw new Error(
      `${name} must be a positive integer`
    );
  }

  return parsedValue;
}

function readMode(): LabMode {
  const mode =
    process.env.LAB_POOL_MODE ?? "unbounded";

  if (
    mode !== "unbounded" &&
    mode !== "bounded"
  ) {
    throw new Error(
      "LAB_POOL_MODE must be unbounded or bounded"
    );
  }

  return mode;
}

async function executeTask(
  pool: Pool,
  taskId: number,
  queryDurationMs: number
): Promise<TaskResult> {
  const waitStartedAt = Date.now();
  let client: PoolClient | undefined;

  logger.info(
    { taskId },
    "Task waiting for database connection"
  );

  try {
    client = await pool.connect();

    const waitMs =
      Date.now() - waitStartedAt;

    logger.info(
      {
        taskId,
        waitMs,
        processId: client.processID
      },
      "Task acquired database connection"
    );

    await client.query(
      `
        SELECT pg_sleep(
          $1::double precision
        )
      `,
      [queryDurationMs / 1000]
    );

    logger.info(
      {
        taskId,
        processId: client.processID
      },
      "Task query completed"
    );

    return {
      taskId,
      succeeded: true,
      waitMs
    };
  } catch (error) {
    const waitMs =
      Date.now() - waitStartedAt;

    logger.warn(
      {
        taskId,
        waitMs,
        err: error
      },
      "Task failed to acquire or use connection"
    );

    return {
      taskId,
      succeeded: false,
      waitMs
    };
  } finally {
    if (client) {
      client.release();

      logger.info(
        { taskId },
        "Task released database connection"
      );
    }
  }
}

async function runWithConcurrencyLimit<T>(
  values: T[],
  concurrency: number,
  worker: (value: T) => Promise<void>
): Promise<void> {
  let nextIndex = 0;

  async function runWorker(): Promise<void> {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= values.length) {
        return;
      }

      await worker(values[currentIndex]!);
    }
  }

  const workers = Array.from(
    {
      length: Math.min(
        concurrency,
        values.length
      )
    },
    () => runWorker()
  );

  await Promise.all(workers);
}

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const mode = readMode();

  const poolMax =
    readPositiveInteger(
      "LAB_POOL_MAX",
      2
    );

  const taskCount =
    readPositiveInteger(
      "LAB_TASK_COUNT",
      5
    );

  const queryDurationMs =
    readPositiveInteger(
      "LAB_QUERY_DURATION_MS",
      4000
    );

  const acquireTimeoutMs =
    readPositiveInteger(
      "LAB_ACQUIRE_TIMEOUT_MS",
      1000
    );

  const pool = new Pool({
    connectionString: databaseUrl,

    max: poolMax,

    connectionTimeoutMillis:
      acquireTimeoutMs,

    idleTimeoutMillis: 10_000,

    application_name:
      "pool-saturation-lab"
  });

  const taskIds = Array.from(
    { length: taskCount },
    (_, index) => index + 1
  );

  const results: TaskResult[] = [];

  logger.info(
    {
      mode,
      poolMax,
      taskCount,
      queryDurationMs,
      acquireTimeoutMs
    },
    "Starting connection-pool lab"
  );

  const metricsTimer = setInterval(() => {
    logger.info(
      {
        totalConnections: pool.totalCount,
        idleConnections: pool.idleCount,
        waitingCallers: pool.waitingCount
      },
      "Pool statistics"
    );
  }, 250);

  try {
    if (mode === "unbounded") {
      const unboundedResults =
        await Promise.all(
          taskIds.map((taskId) =>
            executeTask(
              pool,
              taskId,
              queryDurationMs
            )
          )
        );

      results.push(...unboundedResults);
    } else {
      await runWithConcurrencyLimit(
        taskIds,
        poolMax,

        async (taskId) => {
          const result = await executeTask(
            pool,
            taskId,
            queryDurationMs
          );

          results.push(result);
        }
      );
    }
  } finally {
    clearInterval(metricsTimer);
    await pool.end();
  }

  const succeeded =
    results.filter(
      (result) => result.succeeded
    ).length;

  const failed =
    results.length - succeeded;

  logger.info(
    {
      mode,
      succeeded,
      failed
    },
    "Connection-pool lab completed"
  );
}

main().catch((error: unknown) => {
  logger.fatal(
    { err: error },
    "Connection-pool lab failed"
  );

  process.exitCode = 1;
});
