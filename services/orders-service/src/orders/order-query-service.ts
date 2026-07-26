import type { Pool } from "pg";

import { AppError } from "../errors";
import { decodeOrderCursor, encodeOrderCursor } from "./order-cursor";
import type { ListOrdersInput, OrderStatus } from "./order-list-schema";

interface OrderListRow {
  id: string;
  tenant_id: string;
  external_id: string;
  status: OrderStatus;
  total_minor: string;
  currency: string;
  created_at: Date;
}

export interface ListedOrder {
  id: string;
  tenantId: string;
  externalId: string;
  status: OrderStatus;
  totalMinor: string;
  currency: string;
  createdAt: string;
}

export interface OrderPage {
  items: ListedOrder[];
  nextCursor: string | null;
}

export async function listOrders(
  pool: Pool,
  input: ListOrdersInput,
): Promise<OrderPage> {
  const values: unknown[] = [input.tenantId];

  const predicates = ["tenant_id = $1"];

  if (input.status) {
    values.push(input.status);

    predicates.push(`status = $${values.length}`);
  }

  if (input.cursor) {
    const cursor = decodeOrderCursor(input.cursor);

    const requestedStatus = input.status ?? null;

    if (
      cursor.tenantId !== input.tenantId ||
      cursor.status !== requestedStatus
    ) {
      throw new AppError(
        400,
        "cursor_scope_mismatch",
        "The cursor does not belong to this query",
      );
    }

    values.push(cursor.createdAt, cursor.id);

    const createdAtParameter = values.length - 1;

    const idParameter = values.length;

    predicates.push(`
      (
        created_at,
        id
      ) < (
        $${createdAtParameter}::timestamptz,
        $${idParameter}::uuid
      )
    `);
  }

  values.push(input.limit + 1);

  const limitParameter = values.length;

  const result = await pool.query<OrderListRow>(
    `
        SELECT
          id,
          tenant_id,
          external_id,
          status,
          total_minor,
          currency,
          created_at
        FROM orders
        WHERE ${predicates.join(" AND ")}
        ORDER BY
          created_at DESC,
          id DESC
        LIMIT $${limitParameter}
      `,
    values,
  );

  const hasMore = result.rows.length > input.limit;

  const pageRows = result.rows.slice(0, input.limit);

  const items = pageRows.map(
    (row): ListedOrder => ({
      id: row.id,
      tenantId: row.tenant_id,
      externalId: row.external_id,
      status: row.status,
      totalMinor: row.total_minor,
      currency: row.currency,
      createdAt: row.created_at.toISOString(),
    }),
  );

  const lastRow = pageRows.at(-1);

  const nextCursor =
    hasMore && lastRow
      ? encodeOrderCursor({
          tenantId: input.tenantId,
          status: input.status ?? null,
          createdAt: lastRow.created_at.toISOString(),
          id: lastRow.id,
        })
      : null;

  return {
    items,
    nextCursor,
  };
}
