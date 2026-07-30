# Repository Audit - 2026-07-29

## Scope

This audit captures the current repository layout and Docker/runtime state before Phase 4 productionisation work.

Notes:
- `tree` is not installed in this environment, so the requested tree view is reconstructed with `find` using the same practical exclusions.
- `docker compose ps` required access to the local Docker daemon and was captured successfully on 2026-07-29.

## Presence Check Against Expected Top Level Map

Present:
- `docker-compose.yml`
- `services/`
- `database/`
- `database/migrations/`

Not present:
- root `package.json`
- `pnpm-workspace.yaml`
- `workers/`
- `packages/`
- `kafka/`
- `docker/`

Observation:
- The active platform is currently centered on a single Node service at `services/orders-service` plus shared SQL assets in `database/`.
- There is no monorepo workspace manifest at the repository root. The service uses `npm` with a local `package-lock.json`.

## Reconstructed Tree (`-L 4`, excluding `node_modules|dist|coverage|.git|*.log`)

```text
.
├── .gitignore
├── KAFKA_REMAINING_MODULES.md
├── database
│   ├── migrations
│   │   ├── 001_initial_commerce_schema.sql
│   │   ├── 002_add_order_request_fingerprint.sql
│   │   ├── 003_add_order_pagination_index.sql
│   │   ├── 004_add_high_value_order_index.sql
│   │   ├── 005_add_product_version.sql
│   │   ├── 006_add_cache_invalidation_outbox.sql
│   │   ├── 007_add_background_job_outbox.sql
│   │   ├── 007_add_kafka_outbox.sql
│   │   ├── 008_add_notification_delivery_ledger.sql
│   │   ├── 009_add_notification_inbox.sql
│   │   └── 010_add_kafka_dead_letter.sql
│   └── seeds
│       ├── development.sql
│       ├── order_pagination_lab.sql
│       ├── order_query_performance_lab.sql
│       └── reset_order_concurrency_lab.sql
├── docker-compose.yml
├── scripts
│   └── setup-test-database.sh
└── services
    └── orders-service
        ├── .dockerignore
        ├── .env
        ├── .env.example
        ├── .env.test
        ├── .env.test.example
        ├── Dockerfile
        ├── package-lock.json
        ├── package.json
        ├── src
        │   ├── app.ts
        │   ├── cache-invalidation-outbox.ts
        │   ├── cache-invalidation-worker.ts
        │   ├── config.ts
        │   ├── consumers
        │   ├── database-query.ts
        │   ├── database.ts
        │   ├── dependency-monitor.ts
        │   ├── distributed-lock.ts
        │   ├── errors.ts
        │   ├── events
        │   ├── jobs
        │   ├── kafka.ts
        │   ├── logger.ts
        │   ├── migrations.ts
        │   ├── notifications
        │   ├── orders
        │   ├── outbox
        │   ├── products
        │   ├── redis.ts
        │   ├── scripts
        │   ├── server.ts
        │   ├── single-flight.ts
        │   ├── transaction.ts
        │   └── workers
        ├── tests
        │   ├── app.test.ts
        │   ├── distributed-lock.test.ts
        │   ├── integration
        │   ├── kafka-dead-letter.test.ts
        │   ├── kafka-retry.test.ts
        │   ├── order-cursor.test.ts
        │   ├── product-cache-stampede.test.ts
        │   ├── product-cache.test.ts
        │   ├── single-flight.test.ts
        │   └── transaction.test.ts
        ├── tsconfig.build.json
        ├── tsconfig.json
        ├── vitest.config.ts
        └── vitest.integration.config.ts
```

## File Inventory Requested

```text
./docker-compose.yml
./services/orders-service/.env.example
./services/orders-service/Dockerfile
./services/orders-service/package-lock.json
./services/orders-service/package.json
./services/orders-service/tsconfig.build.json
./services/orders-service/tsconfig.json
```

## Migration State (`ls -la database/migrations`)

```text
total 88
drwxr-xr-x  13 ahmad  staff   416 Jul 29 09:13 .
drwxr-xr-x   4 ahmad  staff   128 Jul 25 14:20 ..
-rw-r--r--@  1 ahmad  staff  3911 Jul 25 16:23 001_initial_commerce_schema.sql
-rw-r--r--@  1 ahmad  staff   225 Jul 25 16:34 002_add_order_request_fingerprint.sql
-rw-r--r--@  1 ahmad  staff   164 Jul 26 13:54 003_add_order_pagination_index.sql
-rw-r--r--@  1 ahmad  staff   188 Jul 27 09:01 004_add_high_value_order_index.sql
-rw-r--r--@  1 ahmad  staff   161 Jul 27 16:46 005_add_product_version.sql
-rw-r--r--@  1 ahmad  staff  1180 Jul 27 23:52 006_add_cache_invalidation_outbox.sql
-rw-r--r--@  1 ahmad  staff  1178 Jul 28 07:35 007_add_background_job_outbox.sql
-rw-r--r--@  1 ahmad  staff   980 Jul 28 14:25 007_add_kafka_outbox.sql
-rw-r--r--@  1 ahmad  staff  1615 Jul 28 09:02 008_add_notification_delivery_ledger.sql
-rw-r--r--   1 ahmad  staff   956 Jul 28 17:20 009_add_notification_inbox.sql
-rw-r--r--@  1 ahmad  staff  1052 Jul 29 09:13 010_add_kafka_dead_letter.sql
```

Audit note:
- There is a duplicate migration prefix at `007_*`, which is worth resolving before any production migration automation is hardened.

## Docker Compose Service List (`docker compose config --services`)

```text
kafka
postgres
notification-kafka-worker
redis
notification-worker
order-outbox-publisher
orders-service
job-dispatcher
```

## Current Containers (`docker compose ps`)

```text
NAME                                    IMAGE                                                                     COMMAND                  SERVICE                     CREATED         STATUS                 PORTS
orderflow-job-dispatcher-1              sha256:69c9eb9c2e0b04c028063f5e6023b8b1d6b4abdd598736bb5c4cf4cabd8e7423   "docker-entrypoint.s…"   job-dispatcher              20 hours ago    Up 5 hours             3000/tcp
orderflow-kafka-1                       apache/kafka:4.3.1                                                        "/__cacert_entrypoin…"   kafka                       25 hours ago    Up 5 hours (healthy)   9092/tcp
orderflow-notification-kafka-worker-2   orderflow-notification-kafka-worker                                       "docker-entrypoint.s…"   notification-kafka-worker   2 minutes ago   Up 2 minutes           3000/tcp
orderflow-notification-worker-1         sha256:f9e3af539a3560da2a9adbed49e9805485f3e35a9841fe2b0f0dbea26c1791a9   "docker-entrypoint.s…"   notification-worker         20 hours ago    Up 5 hours             3000/tcp
orderflow-order-outbox-publisher-1      sha256:7fa31bee92a7b45c8eabc8eb84e439c216bfc1f3334669faaeee731fd21ec107   "docker-entrypoint.s…"   order-outbox-publisher      20 hours ago    Up 5 hours             3000/tcp
orderflow-orders-service-1              sha256:5ea56ca9945ff66af24cb729e61754b56592488b1b567dff021199b930c8253a   "docker-entrypoint.s…"   orders-service              20 hours ago    Up 5 hours (healthy)   0.0.0.0:3000->3000/tcp, [::]:3000->3000/tcp
orderflow-postgres-1                    postgres:16-alpine                                                        "docker-entrypoint.s…"   postgres                    46 hours ago    Up 5 hours (healthy)   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp
orderflow-redis-1                       redis:7-alpine                                                            "docker-entrypoint.s…"   redis                       46 hours ago    Up 5 hours (healthy)   6379/tcp
```

## Service and Worker Reality Check

Single source tree:
- `services/orders-service`

HTTP service:
- `orders-service`

Worker entrypoints present in source:
- `services/orders-service/src/workers/job-dispatcher.ts`
- `services/orders-service/src/workers/notification-worker.ts`
- `services/orders-service/src/workers/notification-worker.kafka.ts`
- `services/orders-service/src/workers/order-outbox-publisher.ts`

Kafka-related consumers present in source:
- `services/orders-service/src/consumers/kafka-dead-letter.ts`
- `services/orders-service/src/consumers/kafka-retry.ts`
- `services/orders-service/src/consumers/notification-consumer-inbox.ts`

## Package and Workspace State

Root package/workspace files:
- No root `package.json`
- No `pnpm-workspace.yaml`
- No npm workspace config detected at root

Service package:
- Package name: `@orderflow/orders-service`
- Package manager lockfile: `package-lock.json`
- Engine: `node >=22`
- Build: `tsc -p tsconfig.build.json`
- Runtime scripts include:
  - `start`
  - `start:job-dispatcher`
  - `start:notification-worker`
- Missing script:
  - There is no dedicated `start:notification-kafka-worker` or `start:order-outbox-publisher` script, even though Compose starts both via direct `node dist/...` commands.

## Docker and Runtime Wiring

Compose-defined infrastructure:
- `postgres` on internal `postgres:5432`, published as host `5433`
- `redis` on internal `redis:6379`
- `kafka` on internal `kafka:9092`

Build context:
- All app and worker services build from `./services/orders-service`
- Shared Dockerfile: `services/orders-service/Dockerfile`

Dependency readiness:
- `postgres`, `redis`, and `kafka` have healthchecks
- `orders-service` depends only on healthy `postgres`
- `job-dispatcher` depends on healthy `postgres`, `redis`, and `kafka`
- `notification-worker` depends on healthy `redis` and `postgres`
- `notification-kafka-worker` depends on healthy `kafka` and `postgres`
- `order-outbox-publisher` depends on healthy `postgres` and `kafka`

Persistence:
- Persistent volume is defined only for Postgres: `orderflow-postgres-data`
- Redis is explicitly configured without persistence
- Kafka has no named volume in Compose

## Existing Productionisation Work Already Present

Already present:
- Multi-stage Dockerfile
- Runtime image separated from build stage
- Non-root runtime user (`USER node`)
- Healthchecks for `postgres`, `redis`, `kafka`, and `orders-service`
- `init: true` on some services
- Graceful stop periods on app and Kafka worker services
- `depends_on` with `condition: service_healthy`
- Persistent volume for Postgres

Still absent or incomplete from a Phase 4 productionisation perspective:
- No root workspace or multi-service build orchestration
- No Compose resource limits
- No explicit custom networks
- No Redis persistence volume
- No Kafka persistence volume
- No healthchecks for worker-only services
- No explicit restart policy on `orders-service`
- No `read_only`, dropped capabilities, tmpfs, or stronger runtime hardening

## Key File Excerpts

### `services/orders-service/Dockerfile`

```dockerfile
FROM node:22-alpine AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM dependencies AS build
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS production-dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev \
    && npm cache clean --force

FROM node:22-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=production-dependencies --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chown=node:node package.json ./
USER node
EXPOSE 3000
CMD ["node", "dist/server.js"]
```

### `services/orders-service/package.json`

```json
{
  "name": "@orderflow/orders-service",
  "private": true,
  "main": "dist/server.js",
  "type": "commonjs",
  "engines": {
    "node": ">=22"
  }
}
```

### `services/orders-service/.env.example`

```dotenv
NODE_ENV=development
SERVICE_NAME=orders-service
PORT=3000
DATABASE_URL=postgresql://orderflow:orderflow-dev@localhost:5433/orderflow
REDIS_URL=redis://localhost:6380
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=orders-service
KAFKA_ORDER_CREATED_TOPIC=order.created.v1
```

