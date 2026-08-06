import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";

const heartbeatFileEnv = process.env.WORKER_HEARTBEAT_FILE;

if (!heartbeatFileEnv) {
  throw new Error(
    "WORKER_HEARTBEAT_FILE is required when loading heartbeat-bootstrap",
  );
}

const heartbeatFile: string = heartbeatFileEnv;

const intervalMs = Number.parseInt(
  process.env.WORKER_HEARTBEAT_INTERVAL_MS ?? "5000",
  10,
);

if (!Number.isFinite(intervalMs) || intervalMs < 1_000) {
  throw new Error("WORKER_HEARTBEAT_INTERVAL_MS must be at least 1000");
}

mkdirSync(path.dirname(heartbeatFile), {
  recursive: true,
});

function writeHeartbeat(): void {
  writeFileSync(
    heartbeatFile,
    JSON.stringify({
      pid: process.pid,
      timestamp: new Date().toISOString(),
    }),
    "utf8",
  );
}

writeHeartbeat();

const timer = setInterval(writeHeartbeat, intervalMs);

// The heartbeat timer alone must not keep a shutting-down worker alive.
timer.unref();
