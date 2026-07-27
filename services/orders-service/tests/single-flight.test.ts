import { describe, expect, it, vi } from "vitest";

import { SingleFlight } from "../src/single-flight";

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

describe("SingleFlight", () => {
  it("coalesces concurrent work for one key", async () => {
    const coordinator = new SingleFlight();

    const task = vi.fn(async () => {
      await delay(50);
      return "product";
    });

    const results = await Promise.all(
      Array.from({ length: 20 }, () => coordinator.run("product-1", task)),
    );

    expect(task).toHaveBeenCalledTimes(1);

    expect(results.every((result) => result.value === "product")).toBe(true);

    expect(results.filter((result) => !result.shared)).toHaveLength(1);

    expect(results.filter((result) => result.shared)).toHaveLength(19);

    expect(coordinator.pendingCount).toBe(0);
  });

  it("does not combine work for different keys", async () => {
    const coordinator = new SingleFlight();

    const task = vi.fn(async (value: string) => {
      await delay(20);
      return value;
    });

    await Promise.all([
      coordinator.run("product-1", () => task("one")),

      coordinator.run("product-2", () => task("two")),
    ]);

    expect(task).toHaveBeenCalledTimes(2);
  });

  it("removes failed work so later calls can retry", async () => {
    const coordinator = new SingleFlight();

    const task = vi
      .fn()
      .mockRejectedValueOnce(new Error("temporary failure"))
      .mockResolvedValueOnce("recovered");

    await expect(coordinator.run("product-1", task)).rejects.toThrow(
      "temporary failure",
    );

    const second = await coordinator.run("product-1", task);

    expect(second.value).toBe("recovered");

    expect(task).toHaveBeenCalledTimes(2);
  });
});
