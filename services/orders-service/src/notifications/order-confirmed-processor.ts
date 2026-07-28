import type { Pool } from "pg";
import type { Logger } from "pino";

import type { OrderConfirmedJob } from "../jobs/bullmq";

import {
  createOrderNotificationKey,
  markNotificationDelivered,
  recordNotificationAttempt,
  recordNotificationFailure,
  reserveNotificationDelivery,
} from "./notification-delivery";

import { sendOrderConfirmedNotification } from "./simulated-provider";

interface ProcessNotificationInput {
  pool: Pool;
  logger: Logger;

  jobId: string;
  data: OrderConfirmedJob;

  simulateCrashAfterProviderSuccess: boolean;
}

export interface ProcessNotificationResult {
  skipped: boolean;
  providerCreatedNow: boolean;
  providerMessageId: string;
}

export async function processOrderConfirmedNotification({
  pool,
  logger,
  jobId,
  data,
  simulateCrashAfterProviderSuccess,
}: ProcessNotificationInput): Promise<ProcessNotificationResult> {
  const idempotencyKey = createOrderNotificationKey(
    data.tenantId,
    data.orderId,
  );

  const delivery = await reserveNotificationDelivery(pool, {
    idempotencyKey,
    tenantId: data.tenantId,
    orderId: data.orderId,
    sourceJobId: jobId,
  });

  if (delivery.status === "delivered" && delivery.providerMessageId) {
    logger.info(
      {
        jobId,
        orderId: data.orderId,
        idempotencyKey,
        providerMessageId: delivery.providerMessageId,
      },
      "Notification already delivered",
    );

    return {
      skipped: true,
      providerCreatedNow: false,
      providerMessageId: delivery.providerMessageId,
    };
  }

  await recordNotificationAttempt(pool, idempotencyKey);

  try {
    const providerResult = await sendOrderConfirmedNotification(pool, {
      idempotencyKey,
      tenantId: data.tenantId,
      orderId: data.orderId,
      externalId: data.externalId,
      totalMinor: data.totalMinor,
      currency: data.currency,
    });

    logger.info(
      {
        jobId,
        orderId: data.orderId,
        idempotencyKey,
        providerMessageId: providerResult.messageId,
        providerCreatedNow: providerResult.createdNow,
      },
      "Notification provider accepted request",
    );

    if (simulateCrashAfterProviderSuccess) {
      throw new Error("Simulated worker crash after provider success");
    }

    await markNotificationDelivered(pool, {
      idempotencyKey,
      providerMessageId: providerResult.messageId,
    });

    return {
      skipped: false,
      providerCreatedNow: providerResult.createdNow,
      providerMessageId: providerResult.messageId,
    };
  } catch (error) {
    await recordNotificationFailure(pool, {
      idempotencyKey,
      error,
    });

    throw error;
  }
}
