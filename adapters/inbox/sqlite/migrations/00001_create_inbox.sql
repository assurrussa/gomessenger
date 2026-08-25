CREATE TABLE IF NOT EXISTS {{inbox}}
(
    consumer_id  TEXT      NOT NULL,
    source       TEXT      NOT NULL,
    message_id   TEXT      NOT NULL,
    fingerprint  BLOB      NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NULL,
    PRIMARY KEY (consumer_id, source, message_id)
);

CREATE INDEX IF NOT EXISTS {{completed_at_index}}
    ON {{inbox}} (completed_at)
    WHERE completed_at IS NOT NULL;
