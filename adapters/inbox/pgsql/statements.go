package pgsql

type statements struct {
	insertIdentity                     string
	insertIdentityAndAttempt           string
	insertIdentityAndAttemptGeneration string
	markComplete                       string
	markTerminal                       string
	markTerminalGeneration             string
	lockIdentity                       string
	readIdentity                       string
	insertAttempt                      string
	insertAttemptGeneration            string
	incrementAttempt                   string
	incrementAttemptGeneration         string
	readAttempt                        string
	readAttemptGeneration              string
	deleteAttempt                      string
	deleteAttemptGeneration            string
	hasAttempts                        string
	deleteIncompleteIdentity           string
	pruneAttemptGenerations            string
	pruneAttempts                      string
	pruneInbox                         string
	lockPruneBatch                     string
}

func newStatements(names namespace) statements {
	return statements{
		insertIdentity: names.render(`INSERT INTO {{inbox}}
        (consumer_id, source, message_id, fingerprint, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING`),
		insertIdentityAndAttempt: names.render(`WITH inserted_identity AS (
        INSERT INTO {{inbox}}
            (consumer_id, source, message_id, fingerprint, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING
        RETURNING consumer_id, source, message_id
    )
    INSERT INTO {{attempts}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
    SELECT consumer_id, source, message_id, $4, 1, $5
    FROM inserted_identity`),
		insertIdentityAndAttemptGeneration: names.render(`WITH inserted_identity AS (
        INSERT INTO {{inbox}}
            (consumer_id, source, message_id, fingerprint, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING
        RETURNING consumer_id, source, message_id
    )
    INSERT INTO {{attempt_generations}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
    SELECT consumer_id, source, message_id, $6, 1, $5
    FROM inserted_identity`),
		markComplete: names.render(`UPDATE {{inbox}} SET completed_at = $1
        WHERE consumer_id = $2 AND source = $3 AND message_id = $4`),
		markTerminal: names.render(`UPDATE {{attempts}}
        SET terminal = TRUE, updated_at = $1
        WHERE consumer_id = $2 AND source = $3 AND message_id = $4`),
		markTerminalGeneration: names.render(`UPDATE {{attempt_generations}}
        SET terminal = TRUE, updated_at = $1
        WHERE consumer_id = $2 AND source = $3 AND message_id = $4 AND fingerprint = $5`),
		lockIdentity: names.render(`SELECT fingerprint, completed_at
        FROM {{inbox}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3
        FOR UPDATE`),
		readIdentity: names.render(`SELECT fingerprint, completed_at
        FROM {{inbox}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3`),
		insertAttempt: names.render(`INSERT INTO {{attempts}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
        VALUES ($1, $2, $3, $4, 1, $5)`),
		insertAttemptGeneration: names.render(`INSERT INTO {{attempt_generations}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
        VALUES ($1, $2, $3, $4, 1, $5)`),
		incrementAttempt: names.render(`UPDATE {{attempts}}
        SET attempts = attempts + 1, updated_at = $1
        WHERE consumer_id = $2 AND source = $3 AND message_id = $4`),
		incrementAttemptGeneration: names.render(`UPDATE {{attempt_generations}}
        SET attempts = attempts + 1, updated_at = $1
        WHERE consumer_id = $2 AND source = $3 AND message_id = $4 AND fingerprint = $5`),
		readAttempt: names.render(`SELECT attempts, terminal
        FROM {{attempts}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3
        FOR UPDATE`),
		readAttemptGeneration: names.render(`SELECT attempts, terminal
        FROM {{attempt_generations}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3 AND fingerprint = $4
        FOR UPDATE`),
		deleteAttempt: names.render(`DELETE FROM {{attempts}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3`),
		deleteAttemptGeneration: names.render(`DELETE FROM {{attempt_generations}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3 AND fingerprint = $4`),
		hasAttempts: names.render(`SELECT
        EXISTS(SELECT 1 FROM {{attempts}}
            WHERE consumer_id = $1 AND source = $2 AND message_id = $3)
        OR EXISTS(SELECT 1 FROM {{attempt_generations}}
            WHERE consumer_id = $1 AND source = $2 AND message_id = $3)`),
		deleteIncompleteIdentity: names.render(`DELETE FROM {{inbox}}
        WHERE consumer_id = $1 AND source = $2 AND message_id = $3 AND completed_at IS NULL`),
		pruneAttemptGenerations: names.render(`WITH doomed AS (
        SELECT consumer_id, source, message_id
        FROM {{inbox}}
        WHERE completed_at < $1
        ORDER BY completed_at, consumer_id, source, message_id
        LIMIT $2
    )
    DELETE FROM {{attempt_generations}} AS attempts
    USING doomed
    WHERE attempts.consumer_id = doomed.consumer_id
      AND attempts.source = doomed.source
      AND attempts.message_id = doomed.message_id`),
		pruneAttempts: names.render(`WITH doomed AS (
        SELECT consumer_id, source, message_id
        FROM {{inbox}}
        WHERE completed_at < $1
        ORDER BY completed_at, consumer_id, source, message_id
        LIMIT $2
    )
    DELETE FROM {{attempts}} AS attempts
    USING doomed
    WHERE attempts.consumer_id = doomed.consumer_id
      AND attempts.source = doomed.source
      AND attempts.message_id = doomed.message_id`),
		pruneInbox: names.render(`WITH doomed AS (
        SELECT consumer_id, source, message_id
        FROM {{inbox}}
        WHERE completed_at < $1
        ORDER BY completed_at, consumer_id, source, message_id
        LIMIT $2
    )
    DELETE FROM {{inbox}} AS inbox
    USING doomed
    WHERE inbox.consumer_id = doomed.consumer_id
      AND inbox.source = doomed.source
      AND inbox.message_id = doomed.message_id`),
		lockPruneBatch: names.render(`SELECT 1
        FROM {{inbox}}
        WHERE completed_at < $1
        ORDER BY completed_at, consumer_id, source, message_id
        LIMIT $2
        FOR UPDATE`),
	}
}
