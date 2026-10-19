\ir 01_extract_bronze.sql
\ir 02_load_silver.sql
\ir 03_load_gold.sql

-- The three runs just made.
SELECT run_id, job_name, status, rows_read, rows_loaded, rows_rejected,
       finished_at - started_at AS duration
FROM ops.etl_run
ORDER BY run_id DESC
LIMIT 3;
