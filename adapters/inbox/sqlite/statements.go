package sqlite

type statements struct {
	insertIdentity             string
	markComplete               string
	markTerminal               string
	markTerminalGeneration     string
	readIdentity               string
	insertAttempt              string
	insertAttemptGeneration    string
	incrementAttempt           string
	incrementAttemptGeneration string
	readAttempt                string
	readAttemptGeneration      string
	deleteAttempt              string
	deleteAttemptGeneration    string
	hasAttempts                string
	deleteIncompleteIdentity   string
	pruneAttemptGenerations    string
	pruneAttempts              string
	pruneInbox                 string
}

func newStatements(names namespace) statements {
	return statements{
		insertIdentity: names.render(`INSERT INTO {{inbox}}
        (consumer_id, source, message_id, fingerprint, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (consumer_id, source, message_id) DO NOTHING`),
		markComplete: names.render(`UPDATE {{inbox}} SET completed_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		markTerminal: names.render(`UPDATE {{attempts}}
        SET terminal = 1, updated_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		markTerminalGeneration: names.render(`UPDATE {{attempt_generations}}
        SET terminal = 1, updated_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`),
		readIdentity: names.render(`SELECT fingerprint, completed_at
        FROM {{inbox}}
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		insertAttempt: names.render(`INSERT INTO {{attempts}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
        VALUES (?, ?, ?, ?, 1, ?)`),
		insertAttemptGeneration: names.render(`INSERT INTO {{attempt_generations}}
        (consumer_id, source, message_id, fingerprint, attempts, updated_at)
        VALUES (?, ?, ?, ?, 1, ?)`),
		incrementAttempt: names.render(`UPDATE {{attempts}}
        SET attempts = attempts + 1, updated_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		incrementAttemptGeneration: names.render(`UPDATE {{attempt_generations}}
        SET attempts = attempts + 1, updated_at = ?
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`),
		readAttempt: names.render(`SELECT attempts, terminal
        FROM {{attempts}}
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		readAttemptGeneration: names.render(`SELECT attempts, terminal
        FROM {{attempt_generations}}
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`),
		deleteAttempt: names.render(`DELETE FROM {{attempts}}
        WHERE consumer_id = ? AND source = ? AND message_id = ?`),
		deleteAttemptGeneration: names.render(`DELETE FROM {{attempt_generations}}
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND fingerprint = ?`),
		hasAttempts: names.render(`SELECT
        EXISTS(SELECT 1 FROM {{attempts}}
            WHERE consumer_id = ? AND source = ? AND message_id = ?)
        OR EXISTS(SELECT 1 FROM {{attempt_generations}}
            WHERE consumer_id = ? AND source = ? AND message_id = ?)`),
		deleteIncompleteIdentity: names.render(`DELETE FROM {{inbox}}
        WHERE consumer_id = ? AND source = ? AND message_id = ? AND completed_at IS NULL`),
		pruneAttemptGenerations: names.render(`DELETE FROM {{attempt_generations}}
        WHERE (consumer_id, source, message_id) IN (
            SELECT consumer_id, source, message_id FROM {{inbox}}
            WHERE completed_at < ?
            ORDER BY completed_at, consumer_id, source, message_id
            LIMIT ?
        )`),
		pruneAttempts: names.render(`DELETE FROM {{attempts}}
        WHERE (consumer_id, source, message_id) IN (
            SELECT consumer_id, source, message_id FROM {{inbox}}
            WHERE completed_at < ?
            ORDER BY completed_at, consumer_id, source, message_id
            LIMIT ?
        )`),
		pruneInbox: names.render(`DELETE FROM {{inbox}}
        WHERE rowid IN (
            SELECT rowid FROM {{inbox}}
            WHERE completed_at < ?
            ORDER BY completed_at, consumer_id, source, message_id
            LIMIT ?
        )`),
	}
}
