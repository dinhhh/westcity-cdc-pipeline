-- =============================================================================
-- File:     etl/01_extract_bronze.sql
-- Purpose:  EXTRACT: copy new and changed rows from identity_fraud, crm and
--           loyalty into the bronze layer of westcity_dw, unchanged.
--           (core_banking customers and accounts do not need extracting:
--           consumer-dw lands their events in bronze.core_event from Kafka.
--           Only the small core product table is copied here.)
-- Run:      step 1 of 3 (etl/run_all.sql runs all three), any time, as
--           often as needed:
--             docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/01_extract_bronze.sql
--
-- How "only new or changed rows" works (incremental extract):
--   ops.extract_watermark remembers, per source table, the highest id or the
--   latest synced_at copied so far. Each run copies only rows above it, then
--   moves the watermark up. Small reference tables, and tables without a
--   usable watermark column, are copied in full instead.
--   The source tables are read through postgres_fdw (sql/08): in this
--   database, crm.crm_case is the crm_case table of the crm database.
--
-- Lab simplification: an id or time watermark can miss a row whose
-- transaction commits AFTER a later row was already copied. Production would
-- use CDC (log positions) or a safety overlap window.
--
-- The whole script is ONE transaction: if any step fails, nothing is copied
-- and the watermarks do not move, so it can simply be run again.
-- =============================================================================

\set ON_ERROR_STOP on
\connect westcity_dw

BEGIN;

-- Open the audit record of this run; run_id is stamped on every copied row.
INSERT INTO ops.etl_run (job_name, script_version)
VALUES ('01_extract_bronze', 'v1')
RETURNING run_id \gset


-- #############################################################################
-- 1. Full copies (small or without a watermark): replace the bronze copy.
-- #############################################################################

-- core_banking.product: product names for the product dimension.
TRUNCATE bronze.core_product;
INSERT INTO bronze.core_product (product_code, product_name, product_type, run_id)
SELECT product_code, product_name, product_type, :run_id
FROM core_banking.product;

-- loyalty.loyalty_offer: welcome offer points.
TRUNCATE bronze.loyalty_offer;
INSERT INTO bronze.loyalty_offer (offer_id, product_code, bonus_points, run_id)
SELECT offer_id, product_code, bonus_points, :run_id
FROM loyalty.loyalty_offer;

-- identity_fraud.onboarding_session: which device each customer used.
TRUNCATE bronze.fraud_onboarding_session;
INSERT INTO bronze.fraud_onboarding_session (session_id, customer_id, device_id, started_at, run_id)
SELECT session_id, customer_id, device_id, started_at, :run_id
FROM identity_fraud.onboarding_session;


-- #############################################################################
-- 2. Insert-only tables: copy rows whose id is above the watermark.
-- #############################################################################

INSERT INTO bronze.fraud_device (device_id, device_fingerprint, run_id)
SELECT device_id, device_fingerprint, :run_id
FROM identity_fraud.device
WHERE device_id > (SELECT last_id FROM ops.extract_watermark
                   WHERE source_table = 'identity_fraud.device');

INSERT INTO bronze.fraud_kyc_verification (kyc_id, customer_id, attempt_no, result, run_id)
SELECT kyc_id, customer_id, attempt_no, result, :run_id
FROM identity_fraud.kyc_verification
WHERE kyc_id > (SELECT last_id FROM ops.extract_watermark
                WHERE source_table = 'identity_fraud.kyc_verification');

INSERT INTO bronze.fraud_watchlist_screening (screening_id, customer_id, match_status, run_id)
SELECT screening_id, customer_id, match_status, :run_id
FROM identity_fraud.watchlist_screening
WHERE screening_id > (SELECT last_id FROM ops.extract_watermark
                      WHERE source_table = 'identity_fraud.watchlist_screening');

INSERT INTO bronze.fraud_risk_assessment (assessment_id, account_id, customer_id, risk_score,
                                          risk_band, decision, reason_codes, assessed_at, run_id)
SELECT assessment_id, account_id, customer_id, risk_score,
       risk_band, decision, reason_codes, assessed_at, :run_id
FROM identity_fraud.risk_assessment
WHERE assessment_id > (SELECT last_id FROM ops.extract_watermark
                       WHERE source_table = 'identity_fraud.risk_assessment');

INSERT INTO bronze.crm_case (case_id, account_id, case_type, case_status, opened_at, run_id)
SELECT case_id, account_id, case_type, case_status, opened_at, :run_id
FROM crm.crm_case
WHERE case_id > (SELECT last_id FROM ops.extract_watermark
                 WHERE source_table = 'crm.crm_case');


-- #############################################################################
-- 3. Updatable tables: copy rows changed (synced_at) after the watermark.
--    A row changed twice between runs is copied once, in its latest state;
--    a row changed again later is copied again (silver keeps the history).
-- #############################################################################

INSERT INTO bronze.crm_contact (contact_id, customer_id, segment, source_version, synced_at, run_id)
SELECT contact_id, customer_id, segment, source_version, synced_at, :run_id
FROM crm.crm_contact
WHERE synced_at > (SELECT last_ts FROM ops.extract_watermark
                   WHERE source_table = 'crm.crm_contact');

INSERT INTO bronze.crm_financial_account (fin_account_id, account_id, contact_id, account_status,
                                          lifecycle_stage, source_version, synced_at, run_id)
SELECT fin_account_id, account_id, contact_id, account_status,
       lifecycle_stage, source_version, synced_at, :run_id
FROM crm.crm_financial_account
WHERE synced_at > (SELECT last_ts FROM ops.extract_watermark
                   WHERE source_table = 'crm.crm_financial_account');

INSERT INTO bronze.loyalty_member (member_id, customer_id, member_status, tier, synced_at, run_id)
SELECT member_id, customer_id, member_status, tier, synced_at, :run_id
FROM loyalty.loyalty_member
WHERE synced_at > (SELECT last_ts FROM ops.extract_watermark
                   WHERE source_table = 'loyalty.loyalty_member');

INSERT INTO bronze.loyalty_member_offer (account_id, member_id, offer_id, assigned_at,
                                         redemption_status, source_version, synced_at, run_id)
SELECT account_id, member_id, offer_id, assigned_at,
       redemption_status, source_version, synced_at, :run_id
FROM loyalty.member_offer
WHERE synced_at > (SELECT last_ts FROM ops.extract_watermark
                   WHERE source_table = 'loyalty.member_offer');


-- #############################################################################
-- 4. Move the watermarks up to the highest id / latest time now in bronze.
--    (coalesce keeps the old value when a table has no rows yet.)
-- #############################################################################

UPDATE ops.extract_watermark w
   SET last_id    = coalesce(m.new_id, w.last_id),
       run_id     = :run_id,
       updated_at = now()
  FROM (VALUES ('identity_fraud.device',              (SELECT max(device_id)     FROM bronze.fraud_device)),
               ('identity_fraud.kyc_verification',    (SELECT max(kyc_id)        FROM bronze.fraud_kyc_verification)),
               ('identity_fraud.watchlist_screening', (SELECT max(screening_id)  FROM bronze.fraud_watchlist_screening)),
               ('identity_fraud.risk_assessment',     (SELECT max(assessment_id) FROM bronze.fraud_risk_assessment)),
               ('crm.crm_case',                       (SELECT max(case_id)       FROM bronze.crm_case))
       ) AS m (source_table, new_id)
 WHERE w.source_table = m.source_table;

UPDATE ops.extract_watermark w
   SET last_ts    = coalesce(m.new_ts, w.last_ts),
       run_id     = :run_id,
       updated_at = now()
  FROM (VALUES ('crm.crm_contact',           (SELECT max(synced_at) FROM bronze.crm_contact)),
               ('crm.crm_financial_account', (SELECT max(synced_at) FROM bronze.crm_financial_account)),
               ('loyalty.loyalty_member',    (SELECT max(synced_at) FROM bronze.loyalty_member)),
               ('loyalty.member_offer',      (SELECT max(synced_at) FROM bronze.loyalty_member_offer))
       ) AS m (source_table, new_ts)
 WHERE w.source_table = m.source_table;


-- #############################################################################
-- 5. Close the run with how many rows were copied, and show it.
-- #############################################################################

CREATE TEMP TABLE copied ON COMMIT DROP AS
          SELECT 'bronze.core_product'              AS bronze_table, count(*) AS rows_copied FROM bronze.core_product              WHERE run_id = :run_id
UNION ALL SELECT 'bronze.loyalty_offer',              count(*) FROM bronze.loyalty_offer              WHERE run_id = :run_id
UNION ALL SELECT 'bronze.fraud_onboarding_session',   count(*) FROM bronze.fraud_onboarding_session   WHERE run_id = :run_id
UNION ALL SELECT 'bronze.fraud_device',               count(*) FROM bronze.fraud_device               WHERE run_id = :run_id
UNION ALL SELECT 'bronze.fraud_kyc_verification',     count(*) FROM bronze.fraud_kyc_verification     WHERE run_id = :run_id
UNION ALL SELECT 'bronze.fraud_watchlist_screening',  count(*) FROM bronze.fraud_watchlist_screening  WHERE run_id = :run_id
UNION ALL SELECT 'bronze.fraud_risk_assessment',      count(*) FROM bronze.fraud_risk_assessment      WHERE run_id = :run_id
UNION ALL SELECT 'bronze.crm_case',                   count(*) FROM bronze.crm_case                   WHERE run_id = :run_id
UNION ALL SELECT 'bronze.crm_contact',                count(*) FROM bronze.crm_contact                WHERE run_id = :run_id
UNION ALL SELECT 'bronze.crm_financial_account',      count(*) FROM bronze.crm_financial_account      WHERE run_id = :run_id
UNION ALL SELECT 'bronze.loyalty_member',             count(*) FROM bronze.loyalty_member             WHERE run_id = :run_id
UNION ALL SELECT 'bronze.loyalty_member_offer',       count(*) FROM bronze.loyalty_member_offer       WHERE run_id = :run_id;

UPDATE ops.etl_run
   SET finished_at = now(),
       status      = 'SUCCEEDED',
       rows_read   = (SELECT sum(rows_copied) FROM copied),   -- an extract copies every row it reads
       rows_loaded = (SELECT sum(rows_copied) FROM copied)
 WHERE run_id = :run_id;

SELECT * FROM copied;

COMMIT;
