CREATE TABLE consumer_inbox (
  consumer_name TEXT NOT NULL,
  event_id UUID NOT NULL,
  topic TEXT NOT NULL,
  partition_number INTEGER NOT NULL,
  message_offset BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT consumer_inbox_pkey
    PRIMARY KEY (consumer_name, event_id),

  CONSTRAINT consumer_inbox_consumer_name_not_blank
    CHECK (LENGTH(TRIM(consumer_name)) > 0),

  CONSTRAINT consumer_inbox_topic_not_blank
    CHECK (LENGTH(TRIM(topic)) > 0),

  CONSTRAINT consumer_inbox_event_type_not_blank
    CHECK (LENGTH(TRIM(event_type)) > 0),

  CONSTRAINT consumer_inbox_partition_non_negative
    CHECK (partition_number >= 0),

  CONSTRAINT consumer_inbox_offset_non_negative
    CHECK (message_offset >= 0)
);

CREATE UNIQUE INDEX consumer_inbox_position_unique
  ON consumer_inbox (
    consumer_name,
    topic,
    partition_number,
    message_offset
  );
