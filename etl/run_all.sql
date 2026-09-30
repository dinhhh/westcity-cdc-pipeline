-- =============================================================================
-- File:     etl/run_all.sql
-- Purpose:  Run the whole warehouse load, in order:
--             1. extract   sources -> bronze          (01_extract_bronze.sql)
--             2. transform bronze  -> silver (vault)  (02_load_silver.sql)
--             3. load      silver  -> gold (star)     (03_load_gold.sql)
--           Each step is its own transaction and its own ops.etl_run row.
-- Run:      docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/run_all.sql
--           (the etl/ folder is mounted at /etl in the postgres container)
-- =============================================================================

\ir 01_extract_bronze.sql
\ir 02_load_silver.sql
\ir 03_load_gold.sql

-- The three runs just made.
SELECT run_id, job_name, status, rows_read, rows_loaded, rows_rejected,
       finished_at - started_at AS duration
FROM ops.etl_run
ORDER BY run_id DESC
LIMIT 3;
