import { randomUUID } from "node:crypto";

import { z } from "zod";

export const orderCreatedEventSchema = z.object({
  eventId: z.string().uuid(),
  eventType: z.literal("order.created"),
  eventVersion: z.literal(1),
  occurredAt: z.string().datetime(),

  tenantId: z.string().uuid(),
  orderId: z.string().uuid(),
  externalId: z.string().min(1),

  totalMinor: z.string().regex(/^\d+$/),
  currency: z.string().regex(/^[A-Z]{3}$/),

  items: z.array(
    z.object({
      productId: z.string().uuid(),
      quantity: z.number().int().positive(),
    }),
  ),
});

export type OrderCreatedEvent = z.infer<typeof orderCreatedEventSchema>;

interface CreateOrderCreatedEventInput {
  tenantId: string;
  orderId: string;
  externalId: string;
  totalMinor: string;
  currency: string;

  items: Array<{
    productId: string;
    quantity: number;
  }>;
}

export function createOrderCreatedEvent(
  input: CreateOrderCreatedEventInput,
): OrderCreatedEvent {
  return {
    eventId: randomUUID(),
    eventType: "order.created",
    eventVersion: 1,
    occurredAt: new Date().toISOString(),

    tenantId: input.tenantId,
    orderId: input.orderId,
    externalId: input.externalId,

    totalMinor: input.totalMinor,
    currency: input.currency,
    items: input.items,
  };
}
