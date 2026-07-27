import { z } from "zod";

import { orderStatusSchema } from "./order-list-schema";

export const highValueOrdersQuerySchema = z.object({
  tenantId: z.string().uuid(),

  status: orderStatusSchema.default("confirmed"),

  limit: z.coerce.number().int().min(1).max(100).default(20),
});

export type HighValueOrdersInput = z.infer<typeof highValueOrdersQuerySchema>;
