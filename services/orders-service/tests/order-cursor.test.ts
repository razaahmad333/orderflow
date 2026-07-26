import { describe, expect, it } from "vitest";

import {
  decodeOrderCursor,
  encodeOrderCursor,
} from "../src/orders/order-cursor";

describe("order pagination cursor", () => {
  it("round-trips a valid cursor", () => {
    const original = {
      tenantId: "00000000-0000-4000-8000-000000000001",

      status: "confirmed" as const,

      createdAt: "2026-07-26T10:00:00.000Z",

      id: "20000000-0000-4000-8000-000000000001",
    };

    const encoded = encodeOrderCursor(original);

    expect(decodeOrderCursor(encoded)).toEqual(original);
  });

  it("rejects malformed cursors", () => {
    expect(() => decodeOrderCursor("not-valid")).toThrowError(
      expect.objectContaining({
        code: "invalid_cursor",
      }),
    );
  });
});
