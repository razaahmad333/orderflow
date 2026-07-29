CREATE TABLE kafka_dead_letter (
  consumer_name TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition_number INTEGER NOT NULL,
  message_offset BIGINT NOT NULL,

  key_text TEXT,
  payload_text TEXT,
  headers JSONB NOT NULL DEFAULT '{}'::JSONB,

  error_kind TEXT NOT NULL,
  error_message TEXT NOT NULL,

  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT kafka_dead_letter_pkey
    PRIMARY KEY (
      consumer_name,
      topic,
      partition_number,
      message_offset
    ),

  CONSTRAINT kafka_dead_letter_consumer_name_not_blank
    CHECK (LENGTH(TRIM(consumer_name)) > 0),

  CONSTRAINT kafka_dead_letter_topic_not_blank
    CHECK (LENGTH(TRIM(topic)) > 0),

  CONSTRAINT kafka_dead_letter_partition_non_negative
    CHECK (partition_number >= 0),

  CONSTRAINT kafka_dead_letter_offset_non_negative
    CHECK (message_offset >= 0),

  CONSTRAINT kafka_dead_letter_error_kind_not_blank
    CHECK (LENGTH(TRIM(error_kind)) > 0),

  CONSTRAINT kafka_dead_letter_error_message_not_blank
    CHECK (LENGTH(TRIM(error_message)) > 0)
);
