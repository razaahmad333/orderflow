import { z } from "zod";

export const orderStatusSchema = z.enum([
  "pending",
  "confirmed",
  "cancelled",
  "failed",
]);

export const listOrdersQuerySchema = z.object({
  tenantId: z.string().uuid(),

  status: orderStatusSchema.optional(),

  limit: z.coerce.number().int().min(1).max(100).default(20),

  cursor: z.string().trim().min(1).optional(),
});

export type ListOrdersInput = z.infer<typeof listOrdersQuerySchema>;

export type OrderStatus = z.infer<typeof orderStatusSchema>;
