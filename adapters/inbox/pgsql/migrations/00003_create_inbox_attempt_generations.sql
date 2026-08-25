CREATE TABLE IF NOT EXISTS {{attempt_generations}}
(
    consumer_id TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    message_id  TEXT        NOT NULL,
    fingerprint BYTEA       NOT NULL,
    attempts    BIGINT      NOT NULL CHECK (attempts >= 0),
    terminal    BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_id, source, message_id, fingerprint)
);
