import { describe, expect, it, vi } from "vitest";
import type { DatabaseQuery } from "../src/database-query";

import {
  getProductById,
  type ProductCache,
} from "../src/products/product-service";

import { SingleFlight } from "../src/single-flight";

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

describe("product cache stampede", () => {
  it("loads PostgreSQL once for concurrent misses", async () => {
    const query = vi.fn(async () => {
      await delay(50);

      return {
        command: "SELECT",
        rowCount: 1,
        oid: 0,
        fields: [],

        rows: [
          {
            id: "10000000-0000-4000-8000-000000000001",

            tenant_id: "00000000-0000-4000-8000-000000000001",

            sku: "KEYBOARD-001",
            name: "Mechanical Keyboard",
            price_minor: "1499",
            currency: "GBP",
            active: true,

            updated_at: new Date("2026-07-27T00:00:00.000Z"),
          },
        ],
      };
    });

    const database: DatabaseQuery = {
      query: query as unknown as DatabaseQuery["query"],
    };

    const values = new Map<string, string>();

    const cache: ProductCache = {
      isReady: true,

      async get(key) {
        return values.get(key) ?? null;
      },

      async set(key, value) {
        values.set(key, value);
        return "OK";
      },

      async del(key) {
        return values.delete(key) ? 1 : 0;
      },
    };

    const coordinator = new SingleFlight();

    const responses = await Promise.all(
      Array.from({ length: 20 }, () =>
        getProductById(database, cache, {
          tenantId: "00000000-0000-4000-8000-000000000001",

          productId: "10000000-0000-4000-8000-000000000001",

          cacheTtlSeconds: 60,
          coordinator,
        }),
      ),
    );

    expect(query).toHaveBeenCalledTimes(1);

    expect(
      responses.filter((response) => response.cacheStatus === "MISS"),
    ).toHaveLength(1);

    expect(
      responses.filter((response) => response.cacheStatus === "COALESCED")
        .length,
    ).toBeGreaterThan(0);
  });
});
