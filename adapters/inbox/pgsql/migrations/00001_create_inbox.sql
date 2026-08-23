CREATE TABLE IF NOT EXISTS gomessenger_inbox
(
    consumer_id  TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    message_id   TEXT        NOT NULL,
    fingerprint  BYTEA       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    PRIMARY KEY (consumer_id, source, message_id)
);

CREATE INDEX IF NOT EXISTS gomessenger_inbox_completed_at_idx
    ON gomessenger_inbox (completed_at)
    WHERE completed_at IS NOT NULL;
