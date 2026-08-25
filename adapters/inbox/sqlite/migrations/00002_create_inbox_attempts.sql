CREATE TABLE IF NOT EXISTS {{attempts}}
(
    consumer_id TEXT      NOT NULL,
    source      TEXT      NOT NULL,
    message_id  TEXT      NOT NULL,
    fingerprint BLOB      NOT NULL,
    attempts    INTEGER   NOT NULL CHECK (attempts >= 0),
    terminal    INTEGER   NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    updated_at  TIMESTAMP NOT NULL,
    PRIMARY KEY (consumer_id, source, message_id)
);
