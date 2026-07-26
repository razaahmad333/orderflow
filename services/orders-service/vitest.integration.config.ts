import { config as loadEnvironment } from "dotenv";
import { defineConfig } from "vitest/config";

loadEnvironment({
  path: ".env.test",
});

export default defineConfig({
  test: {
    include: ["tests/integration/**/*.test.ts"],

    fileParallelism: false,

    testTimeout: 15_000,
    hookTimeout: 30_000,
  },
});
