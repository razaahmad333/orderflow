import { describe, expect, it, vi } from "vitest";

import { RedisDistributedLock } from "../src/distributed-lock";

describe("RedisDistributedLock", () => {
  it("uses NX and a bounded lease", async () => {
    const client = {
      set: vi.fn().mockResolvedValue("OK"),
      eval: vi.fn().mockResolvedValue(1),
    };

    const lock = new RedisDistributedLock(client);

    const lease = await lock.acquire("product-key", 3000);

    expect(lease).not.toBeNull();

    expect(client.set).toHaveBeenCalledWith(
      "orderflow:lock:product-key",
      expect.any(String),
      {
        NX: true,
        PX: 3000,
      },
    );

    await lease!.release();

    expect(client.eval).toHaveBeenCalledWith(
      expect.stringContaining(`redis.call("GET", KEYS[1])`),
      {
        keys: ["orderflow:lock:product-key"],
        arguments: [lease!.token],
      },
    );
  });

  it("returns null when another owner holds the lock", async () => {
    const client = {
      set: vi.fn().mockResolvedValue(null),
      eval: vi.fn(),
    };

    const lock = new RedisDistributedLock(client);

    const lease = await lock.acquire("product-key", 3000);

    expect(lease).toBeNull();
    expect(client.eval).not.toHaveBeenCalled();
  });
});
