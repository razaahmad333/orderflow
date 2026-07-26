import { z } from "zod";

import { AppError } from "../errors";
import { orderStatusSchema, type OrderStatus } from "./order-list-schema";

const cursorPayloadSchema = z.object({
  tenantId: z.string().uuid(),

  status: orderStatusSchema.nullable(),

  createdAt: z.string().datetime({
    offset: true,
  }),

  id: z.string().uuid(),
});

export interface OrderCursor {
  tenantId: string;
  status: OrderStatus | null;
  createdAt: string;
  id: string;
}

export function encodeOrderCursor(cursor: OrderCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

export function decodeOrderCursor(encodedCursor: string): OrderCursor {
  try {
    const decoded = Buffer.from(encodedCursor, "base64url").toString("utf8");

    const parsedJson: unknown = JSON.parse(decoded);

    const parsed = cursorPayloadSchema.safeParse(parsedJson);

    if (!parsed.success) {
      throw new Error("Cursor payload validation failed");
    }

    return parsed.data;
  } catch {
    throw new AppError(
      400,
      "invalid_cursor",
      "The pagination cursor is invalid",
    );
  }
}
