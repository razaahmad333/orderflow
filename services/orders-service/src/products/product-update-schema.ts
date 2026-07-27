import { z } from "zod";

export const updateProductParamsSchema = z.object({
  productId: z.string().uuid(),
});

export const updateProductBodySchema = z
  .object({
    tenantId: z.string().uuid(),

    expectedVersion: z
      .string()
      .regex(/^\d+$/, "expectedVersion must be a non-negative integer string"),

    name: z.string().trim().min(1).max(200).optional(),

    priceMinor: z
      .string()
      .regex(/^\d+$/, "priceMinor must be a non-negative integer string")
      .optional(),

    active: z.boolean().optional(),
  })
  .superRefine((value, context) => {
    if (
      value.name === undefined &&
      value.priceMinor === undefined &&
      value.active === undefined
    ) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "At least one product field must be provided",
      });
    }
  });

export type UpdateProductInput = z.infer<typeof updateProductBodySchema> & {
  productId: string;
};
