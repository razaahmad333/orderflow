import type { Producer } from "kafkajs";

import {
  createOrderCreatedEvent,
  type OrderCreatedEvent,
} from "./order-created-event";

interface PublishOrderCreatedInput {
  tenantId: string;
  orderId: string;
  externalId: string;
  totalMinor: string;
  currency: string;

  items: Array<{
    productId: string;
    quantity: number;
  }>;

  requestId?: string;
}

export interface OrderEventPublisher {
  publishOrderCreated(
    input: PublishOrderCreatedInput,
  ): Promise<OrderCreatedEvent>;
}

export function createOrderEventPublisher(
  producer: Producer,
  topic: string,
): OrderEventPublisher {
  return {
    async publishOrderCreated(input): Promise<OrderCreatedEvent> {
      const event = createOrderCreatedEvent(input);

      await producer.send({
        topic,
        acks: -1,
        timeout: 5_000,

        messages: [
          {
            /*
             * Events for one order are routed to the
             * same partition.
             */
            key: event.orderId,

            value: JSON.stringify(event),

            headers: {
              "event-id": event.eventId,
              "event-type": event.eventType,
              "event-version": String(event.eventVersion),

              ...(input.requestId
                ? {
                    "request-id": input.requestId,
                  }
                : {}),
            },
          },
        ],
      });

      return event;
    },
  };
}
