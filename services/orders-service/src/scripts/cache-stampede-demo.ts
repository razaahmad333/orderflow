import { performance } from "node:perf_hooks";

import type { DatabaseQuery } from "../database-query";

import { getProductById, type ProductCache } from "../products/product-service";

import { SingleFlight } from "../single-flight";

const tenantId = "00000000-0000-4000-8000-000000000001";

const productId = "10000000-0000-4000-8000-000000000001";

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function createFakeDatabase(): {
  database: DatabaseQuery;
  getQueryCount(): number;
} {
  let queryCount = 0;

  const database = {
    async query() {
      queryCount += 1;

      await delay(100);

      return {
        command: "SELECT",
        rowCount: 1,
        oid: 0,
        fields: [],

        rows: [
          {
            id: productId,
            tenant_id: tenantId,
            sku: "KEYBOARD-001",
            name: "Mechanical Keyboard",
            price_minor: "1499",
            currency: "GBP",
            active: true,
            updated_at: new Date(),
          },
        ],
      };
    },
  } as unknown as DatabaseQuery;

  return {
    database,

    getQueryCount(): number {
      return queryCount;
    },
  };
}

function createEmptyCache(): ProductCache {
  return {
    isReady: true,

    async get() {
      return null;
    },

    async set() {
      return "OK";
    },

    async del() {
      return 0;
    },
  };
}

async function runScenario(
  name: string,
  coordinator?: SingleFlight,
): Promise<void> {
  const fake = createFakeDatabase();

  const cache = createEmptyCache();

  const startedAt = performance.now();

  const responses = await Promise.all(
    Array.from({ length: 50 }, () =>
      getProductById(fake.database, cache, {
        tenantId,
        productId,
        cacheTtlSeconds: 60,
        coordinator,
      }),
    ),
  );

  const durationMs = Math.round(performance.now() - startedAt);

  const statusCounts = responses.reduce<Record<string, number>>(
    (counts, response) => {
      counts[response.cacheStatus] = (counts[response.cacheStatus] ?? 0) + 1;

      return counts;
    },
    {},
  );

  console.log({
    scenario: name,
    requests: responses.length,
    databaseQueries: fake.getQueryCount(),
    durationMs,
    statusCounts,
  });
}

async function main(): Promise<void> {
  await runScenario("without-single-flight");

  await runScenario("with-single-flight", new SingleFlight());
}

void main();
