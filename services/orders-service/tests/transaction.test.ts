import type {
  Pool,
  PoolClient
} from "pg";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { AppError } from "../src/errors";
import {
  withTransactionRetry
} from "../src/transaction";

function postgresError(code: string): Error & {
  code: string;
} {
  return Object.assign(
    new Error(`PostgreSQL error ${code}`),
    { code }
  );
}

describe("withTransactionRetry", () => {
  it("retries a deadlocked transaction", async () => {
    const firstClient = {
      query: vi.fn().mockResolvedValue({}),
      release: vi.fn()
    } as unknown as PoolClient;

    const secondClient = {
      query: vi.fn().mockResolvedValue({}),
      release: vi.fn()
    } as unknown as PoolClient;

    const pool = {
      connect: vi
        .fn()
        .mockResolvedValueOnce(firstClient)
        .mockResolvedValueOnce(secondClient)
    } as unknown as Pool;

    const operation = vi
      .fn()
      .mockRejectedValueOnce(
        postgresError("40P01")
      )
      .mockResolvedValueOnce("completed");

    const onRetry = vi.fn();

    const result = await withTransactionRetry(
      pool,
      operation,
      {
        maxAttempts: 3,
        baseDelayMs: 10,
        onRetry
      }
    );

    expect(result).toBe("completed");
    expect(operation).toHaveBeenCalledTimes(2);
    expect(pool.connect).toHaveBeenCalledTimes(2);

    expect(firstClient.query).toHaveBeenCalledWith(
      "BEGIN"
    );

    expect(firstClient.query).toHaveBeenCalledWith(
      "ROLLBACK"
    );

    expect(secondClient.query).toHaveBeenCalledWith(
      "COMMIT"
    );

    expect(firstClient.release).toHaveBeenCalled();
    expect(secondClient.release).toHaveBeenCalled();

    expect(onRetry).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        nextAttempt: 2,
        errorCode: "40P01"
      })
    );
  });

  it("does not retry deterministic business errors", async () => {
    const client = {
      query: vi.fn().mockResolvedValue({}),
      release: vi.fn()
    } as unknown as PoolClient;

    const pool = {
      connect: vi.fn().mockResolvedValue(client)
    } as unknown as Pool;

    const operation = vi.fn().mockRejectedValue(
      new AppError(
        409,
        "insufficient_inventory",
        "Insufficient inventory"
      )
    );

    await expect(
      withTransactionRetry(
        pool,
        operation,
        {
          maxAttempts: 3,
          baseDelayMs: 10
        }
      )
    ).rejects.toMatchObject({
      code: "insufficient_inventory"
    });

    expect(operation).toHaveBeenCalledTimes(1);
    expect(pool.connect).toHaveBeenCalledTimes(1);

    expect(client.query).toHaveBeenCalledWith(
      "ROLLBACK"
    );

    expect(client.release).toHaveBeenCalled();
  });

  it("stops after the configured attempt limit", async () => {
    const clients = [1, 2, 3].map(
      () =>
        ({
          query: vi.fn().mockResolvedValue({}),
          release: vi.fn()
        }) as unknown as PoolClient
    );

    const pool = {
      connect: vi
        .fn()
        .mockResolvedValueOnce(clients[0])
        .mockResolvedValueOnce(clients[1])
        .mockResolvedValueOnce(clients[2])
    } as unknown as Pool;

    const operation = vi.fn().mockRejectedValue(
      postgresError("40001")
    );

    await expect(
      withTransactionRetry(
        pool,
        operation,
        {
          maxAttempts: 3,
          baseDelayMs: 10
        }
      )
    ).rejects.toMatchObject({
      code: "40001"
    });

    expect(operation).toHaveBeenCalledTimes(3);
    expect(pool.connect).toHaveBeenCalledTimes(3);
  });
});
