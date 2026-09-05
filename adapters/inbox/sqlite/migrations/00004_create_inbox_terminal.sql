CREATE TABLE IF NOT EXISTS {{terminal}} (
 consumer_id TEXT NOT NULL,
 source TEXT NOT NULL,
 message_id TEXT NOT NULL,
 fingerprint BLOB NOT NULL,
 attempts BIGINT NOT NULL CHECK (attempts > 0),
 failure_kind TEXT NOT NULL CHECK (failure_kind IN ('permanent', 'attempts_exhausted')),
 terminal_at TIMESTAMP NOT NULL,
 handoff_confirmed_at TIMESTAMP,
 PRIMARY KEY (consumer_id, source, message_id, fingerprint)
);
CREATE INDEX IF NOT EXISTS {{terminal_handoff_index}} ON {{terminal}} (handoff_confirmed_at);

-- Existing permanent outcomes remain closed; historical ACKs cannot be inferred.
INSERT INTO {{terminal}} (consumer_id, source, message_id, fingerprint, attempts, failure_kind, terminal_at)
SELECT consumer_id, source, message_id, fingerprint, attempts, 'permanent', updated_at
FROM {{attempts}} WHERE terminal AND attempts > 0
ON CONFLICT (consumer_id, source, message_id, fingerprint) DO NOTHING;
INSERT INTO {{terminal}} (consumer_id, source, message_id, fingerprint, attempts, failure_kind, terminal_at)
SELECT consumer_id, source, message_id, fingerprint, attempts, 'permanent', updated_at
FROM {{attempt_generations}} WHERE terminal AND attempts > 0
ON CONFLICT (consumer_id, source, message_id, fingerprint) DO NOTHING;
