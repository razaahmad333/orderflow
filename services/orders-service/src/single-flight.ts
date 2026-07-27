export interface SingleFlightResult<T> {
  value: T;

  /*
   * false: this caller executed the task.
   * true: this caller joined an existing task.
   */
  shared: boolean;
}

export class SingleFlight {
  private readonly inFlight = new Map<string, Promise<unknown>>();

  get pendingCount(): number {
    return this.inFlight.size;
  }

  async run<T>(
    key: string,
    task: () => Promise<T>,
  ): Promise<SingleFlightResult<T>> {
    const existing = this.inFlight.get(key);

    if (existing) {
      return {
        value: await (existing as Promise<T>),

        shared: true,
      };
    }

    const promise = task();

    this.inFlight.set(key, promise);

    try {
      return {
        value: await promise,
        shared: false,
      };
    } finally {
      /*
       * Delete only if the map still contains this
       * exact Promise. This prevents an older task
       * from deleting a newer task for the same key.
       */
      if (this.inFlight.get(key) === promise) {
        this.inFlight.delete(key);
      }
    }
  }
}
