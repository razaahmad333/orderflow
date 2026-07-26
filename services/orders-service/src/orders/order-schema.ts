import { z } from "zod";

const orderItemSchema = z.object({
  productId: z.string().uuid(),
  quantity: z.number().int().positive().max(1000),
});

export const placeOrderSchema = z
  .object({
    tenantId: z.string().uuid(),

    externalId: z.string().trim().min(1).max(100),

    currency: z.string().regex(/^[A-Z]{3}$/),

    items: z.array(orderItemSchema).min(1).max(100),
  })
  .superRefine((value, context) => {
    const productIds = new Set<string>();

    value.items.forEach((item, index) => {
      if (productIds.has(item.productId)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["items", index, "productId"],
          message: "Duplicate productId",
        });
      }

      productIds.add(item.productId);
    });
  });

export type PlaceOrderInput = z.infer<typeof placeOrderSchema>;
