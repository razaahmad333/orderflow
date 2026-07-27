import { randomUUID } from "node:crypto";

export interface DistributedLockClient {
  set(
    key: string,
    value: string,
    options: {
      NX: true;
      PX: number;
    },
  ): Promise<string | null>;

  eval(
    script: string,
    options: {
      keys: string[];
      arguments: string[];
    },
  ): Promise<unknown>;
}

export interface DistributedLockLease {
  readonly key: string;
  readonly token: string;

  release(): Promise<boolean>;
}

export interface DistributedLock {
  acquire(
    resourceKey: string,
    ttlMs: number,
  ): Promise<DistributedLockLease | null>;
}

const releaseScript = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  end

  return 0
`;

export class RedisDistributedLock implements DistributedLock {
  constructor(private readonly client: DistributedLockClient) {}

  async acquire(
    resourceKey: string,
    ttlMs: number,
  ): Promise<DistributedLockLease | null> {
    const key = `orderflow:lock:${resourceKey}`;

    const token = randomUUID();

    const result = await this.client.set(key, token, {
      NX: true,
      PX: ttlMs,
    });

    if (result !== "OK") {
      return null;
    }

    let released = false;

    return {
      key,
      token,

      release: async (): Promise<boolean> => {
        if (released) {
          return false;
        }

        released = true;

        const deleted = await this.client.eval(releaseScript, {
          keys: [key],
          arguments: [token],
        });

        return Number(deleted) === 1;
      },
    };
  }
}
