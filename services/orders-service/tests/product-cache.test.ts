import { describe, expect, it, vi } from "vitest";

import {
  getProductById,
  type ProductCache,
} from "../src/products/product-service";

interface TestDatabaseQuery {
  query: <Row>(
    text: string,
    values?: readonly unknown[]
  ) => Promise<{
    rows: Row[];
  }>;
}

describe("product cache-aside", () => {
  it("loads PostgreSQL once and then serves Redis", async () => {
    const query = vi.fn().mockResolvedValue({
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
          price_minor: "1299",
          currency: "GBP",
          active: true,

          updated_at: new Date("2026-07-27T00:00:00.000Z"),
        },
      ],
    });

    const database: TestDatabaseQuery = {
      query: query as unknown as TestDatabaseQuery["query"],
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

    const input = {
      tenantId: "00000000-0000-4000-8000-000000000001",

      productId: "10000000-0000-4000-8000-000000000001",

      cacheTtlSeconds: 60,
    };

    const first = await getProductById(database, cache, input);

    const second = await getProductById(database, cache, input);

    expect(first.cacheStatus).toBe("MISS");

    expect(second.cacheStatus).toBe("HIT");

    expect(query).toHaveBeenCalledTimes(1);
  });
});
