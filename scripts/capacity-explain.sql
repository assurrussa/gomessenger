BEGIN;
SET LOCAL statement_timeout = '30s';

INSERT INTO jobs (
    id, queue, name, schema_version, payload, attempts, reserved_at,
    lease_token, deduplication_key, available_at, created_at
)
SELECT md5('gomessenger-capacity-explain-' || value)::uuid,
       'queue', 'gomessenger.relay', 1, '{}', 0, NULL,
       '00000000-0000-0000-0000-000000000000'::uuid, NULL,
       clock_timestamp() - interval '1 second', clock_timestamp()
FROM generate_series(1, 100) AS value;

EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
WITH requested(name, schema_version) AS (
    VALUES ('gomessenger.relay'::text, 1::integer)
), candidates AS (
    SELECT jobs.id
    FROM jobs
    JOIN requested
      ON requested.name = jobs.name
     AND requested.schema_version = jobs.schema_version
    WHERE jobs.available_at <= clock_timestamp()
      AND (jobs.reserved_at IS NULL OR jobs.reserved_at <= clock_timestamp())
    ORDER BY jobs.available_at, jobs.created_at, jobs.id
    LIMIT 100
    FOR UPDATE OF jobs SKIP LOCKED
), updated AS (
    UPDATE jobs
    SET attempts = attempts + 1,
        reserved_at = clock_timestamp() + interval '1 minute',
        lease_token = '00000000-0000-0000-0000-000000000001'::uuid
    FROM candidates
    WHERE candidates.id = jobs.id
    RETURNING jobs.*
)
SELECT * FROM updated ORDER BY available_at, created_at, id;

EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
WITH input AS (
    SELECT id AS job_id,
           CASE row_number() OVER (ORDER BY available_at, created_at, id) % 4
             WHEN 0 THEN 4::smallint
             WHEN 1 THEN 1::smallint
             WHEN 2 THEN 2::smallint
             ELSE 3::smallint
           END AS kind,
           clock_timestamp() + interval '1 minute' AS available_at,
           'capacity EXPLAIN rollback'::text AS reason,
           id AS failed_job_id
    FROM jobs
    WHERE lease_token = '00000000-0000-0000-0000-000000000001'::uuid
    ORDER BY available_at, created_at, id
    LIMIT 100
), owned AS MATERIALIZED (
    SELECT jobs.*, input.kind, input.available_at AS next_available_at,
           input.reason, input.failed_job_id
    FROM jobs
    JOIN input ON input.job_id = jobs.id
    WHERE jobs.lease_token = '00000000-0000-0000-0000-000000000001'::uuid
      AND jobs.reserved_at > clock_timestamp()
    FOR UPDATE OF jobs
), failed AS (
    INSERT INTO jobs_failed (
        id, job_id, connection, queue, name, schema_version, payload,
        reason, exception, failed_at, created_at
    )
    SELECT failed_job_id, id, '', queue, name, schema_version, payload,
           reason, '', clock_timestamp(), clock_timestamp()
    FROM owned WHERE kind = 4
    RETURNING job_id
), deleted AS (
    DELETE FROM jobs USING owned
    WHERE jobs.id = owned.id AND owned.kind IN (1, 4)
    RETURNING jobs.id
), rescheduled AS (
    UPDATE jobs
    SET attempts = CASE WHEN owned.kind = 3 THEN jobs.attempts - 1 ELSE jobs.attempts END,
        available_at = owned.next_available_at,
        reserved_at = NULL,
        lease_token = '00000000-0000-0000-0000-000000000000'::uuid
    FROM owned
    WHERE jobs.id = owned.id AND owned.kind IN (2, 3) AND jobs.attempts > 0
    RETURNING jobs.id
)
SELECT (SELECT count(*) FROM deleted) + (SELECT count(*) FROM rescheduled);

ROLLBACK;
