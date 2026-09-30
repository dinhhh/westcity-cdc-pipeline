-- =============================================================================
-- File:     etl/02_load_silver.sql
-- Purpose:  TRANSFORM (1/2): load the Data Vault in the silver layer from
--           bronze: hubs (business keys), links (relationships) and
--           satellites (attributes with full history).
-- Run:      step 2 of 3, after etl/01_extract_bronze.sql:
--             docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/02_load_silver.sql
--
-- Safe to run again at any time (idempotent):
--   * a hub or link row is only added if its key is new;
--   * a satellite row is only added if the attributes CHANGED since the
--     latest row for that key (compared through hash_diff). Satellites are
--     never updated or deleted, so silver keeps the full history.
--
-- Hash keys: hk_* = md5 of the business key (e.g. md5('42') for customer 42).
-- hash_diff = md5 of ROW(attributes)::text, e.g. md5('(Mia,Nguyen,NSW,t)').
-- (ROW(...)::text keeps an empty slot for a NULL, so moving a value from one
-- column to another always changes the hash.)
--
-- Data quality: rows that fail a rule are moved to ops.dq_quarantine with
-- the reason, and are NOT loaded.
-- One transaction: if any step fails, nothing is loaded.
-- =============================================================================

\set ON_ERROR_STOP on
\connect westcity_dw

BEGIN;

INSERT INTO ops.etl_run (job_name, script_version)
VALUES ('02_load_silver', 'v1')
RETURNING run_id \gset


-- #############################################################################
-- 1. Current state of every core_banking customer and account, read from
--    the Kafka events in bronze.core_event. Each event carries the full
--    snapshot, so the event with the highest version is the current state.
-- #############################################################################

CREATE TEMP TABLE core_customer ON COMMIT DROP AS
SELECT DISTINCT ON (aggregate_id)
       aggregate_id                                                  AS customer_id,
       payload -> 'data' ->> 'first_name'                            AS first_name,
       payload -> 'data' ->> 'last_name'                             AS last_name,
       (payload -> 'data' ->> 'date_of_birth')::date                 AS date_of_birth,
       payload -> 'data' -> 'residential_address' ->> 'state'        AS residential_state,
       (payload -> 'data' ->> 'marketing_consent')::boolean          AS marketing_consent,
       aggregate_version                                             AS source_version,
       occurred_at                                                   AS src_commit_ts,
       payload
FROM bronze.core_event
WHERE aggregate_type = 'customer'
ORDER BY aggregate_id, aggregate_version DESC;

CREATE TEMP TABLE core_account ON COMMIT DROP AS
SELECT DISTINCT ON (aggregate_id)
       aggregate_id                                                  AS account_id,
       payload -> 'data' ->> 'account_status'                        AS account_status,
       payload -> 'data' ->> 'channel'                               AS channel,
       payload -> 'data' ->> 'product_code'                          AS product_code,
       (payload -> 'data' ->> 'opened_at')::timestamptz              AS opened_at,
       (payload -> 'data' ->> 'activated_at')::timestamptz           AS activated_at,  -- NULL if not active
       payload -> 'data' -> 'holders'                                AS holders,
       aggregate_version                                             AS source_version,
       occurred_at                                                   AS src_commit_ts,
       payload
FROM bronze.core_event
WHERE aggregate_type = 'account'
ORDER BY aggregate_id, aggregate_version DESC;


-- #############################################################################
-- 2. Data-quality rules. Each statement MOVES the failing rows out of the
--    batch (DELETE ... RETURNING) and into ops.dq_quarantine.
-- #############################################################################

-- Rule: every account has exactly one PRIMARY holder (who owns it).
WITH rejected AS (
    DELETE FROM core_account a
    WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(a.holders) h
                      WHERE h ->> 'holder_role' = 'PRIMARY')
    RETURNING a.account_id, a.payload
)
INSERT INTO ops.dq_quarantine (run_id, source_table, record_key, rule_name, reason, raw_record)
SELECT :run_id, 'bronze.core_event', 'account ' || account_id,
       'ACCOUNT_HAS_PRIMARY_HOLDER', 'account event has no PRIMARY holder', payload
FROM rejected
ON CONFLICT (source_table, record_key, rule_name) DO NOTHING;

-- Rule: customers are adults (18 or older) and have a date of birth.
WITH rejected AS (
    DELETE FROM core_customer c
    WHERE c.date_of_birth IS NULL
       OR c.date_of_birth > (c.src_commit_ts::date - interval '18 years')
    RETURNING c.customer_id, c.payload
)
INSERT INTO ops.dq_quarantine (run_id, source_table, record_key, rule_name, reason, raw_record)
SELECT :run_id, 'bronze.core_event', 'customer ' || customer_id,
       'CUSTOMER_IS_ADULT', 'date of birth missing or customer younger than 18', payload
FROM rejected
ON CONFLICT (source_table, record_key, rule_name) DO NOTHING;


-- #############################################################################
-- 3. Hubs: one row per business key, added the first time it is seen.
--    Customers and accounts come from core_banking, the system of record.
--    Rows from other systems are only loaded for keys already in a hub
--    (the joins below); anything that arrives early loads on a later run.
-- #############################################################################

INSERT INTO silver.hub_customer (hk_customer, customer_id, load_dts, record_source, run_id)
SELECT md5(customer_id::text), customer_id, now(), 'core_banking (Kafka)', :run_id
FROM core_customer
ON CONFLICT (hk_customer) DO NOTHING;

INSERT INTO silver.hub_account (hk_account, account_id, load_dts, record_source, run_id)
SELECT md5(account_id::text), account_id, now(), 'core_banking (Kafka)', :run_id
FROM core_account
ON CONFLICT (hk_account) DO NOTHING;

INSERT INTO silver.hub_product (hk_product, product_code, load_dts, record_source, run_id)
SELECT md5(product_code), product_code, now(), 'core_banking.product', :run_id
FROM bronze.core_product
ON CONFLICT (hk_product) DO NOTHING;

INSERT INTO silver.hub_device (hk_device, device_fingerprint, load_dts, record_source, run_id)
SELECT DISTINCT md5(device_fingerprint), device_fingerprint, now(), 'identity_fraud.device', :run_id
FROM bronze.fraud_device
ON CONFLICT (hk_device) DO NOTHING;


-- #############################################################################
-- 4. Links: one row per relationship, added the first time it is seen.
-- #############################################################################

-- Account <-> customer (holders), from the account event's holder list.
INSERT INTO silver.link_account_holder (hk_account_holder, hk_account, hk_customer,
                                        holder_role, load_dts, record_source, run_id)
SELECT md5(a.account_id || '|' || hc.customer_id),
       md5(a.account_id::text),
       hc.hk_customer,
       h ->> 'holder_role',
       now(), 'core_banking (Kafka)', :run_id
FROM core_account a
CROSS JOIN LATERAL jsonb_array_elements(a.holders) AS h
JOIN silver.hub_customer hc ON hc.customer_id = (h ->> 'customer_id')::bigint
ON CONFLICT (hk_account_holder) DO NOTHING;

-- Account <-> product.
INSERT INTO silver.link_account_product (hk_account_product, hk_account, hk_product,
                                         load_dts, record_source, run_id)
SELECT md5(a.account_id || '|' || a.product_code),
       md5(a.account_id::text),
       hp.hk_product,
       now(), 'core_banking (Kafka)', :run_id
FROM core_account a
JOIN silver.hub_product hp ON hp.product_code = a.product_code
ON CONFLICT (hk_account_product) DO NOTHING;

-- Customer <-> device, from the onboarding sessions (identity_fraud).
INSERT INTO silver.link_customer_device (hk_customer_device, hk_customer, hk_device,
                                         load_dts, record_source, run_id)
SELECT DISTINCT
       md5(s.customer_id || '|' || d.device_fingerprint),
       hc.hk_customer,
       hd.hk_device,
       now(), 'identity_fraud.onboarding_session', :run_id
FROM bronze.fraud_onboarding_session s
JOIN bronze.fraud_device d  ON d.device_id = s.device_id
JOIN silver.hub_customer hc ON hc.customer_id = s.customer_id
JOIN silver.hub_device hd   ON hd.device_fingerprint = d.device_fingerprint
ON CONFLICT (hk_customer_device) DO NOTHING;


-- #############################################################################
-- 5. Satellites. Same pattern for each one:
--      new = the current attributes of every key, with their hash_diff
--      insert the rows of "new" whose hash_diff differs from the LATEST
--      satellite row for that key (or that have no satellite row yet).
-- #############################################################################

-- 5a. Customer details (core_banking).
INSERT INTO silver.sat_customer_details (hk_customer, load_dts, hash_diff, first_name, last_name,
                                         date_of_birth, residential_state, marketing_consent,
                                         source_version, src_commit_ts, record_source, run_id)
SELECT new.*, 'core_banking (Kafka)', :run_id
FROM (SELECT md5(c.customer_id::text) AS hk_customer,
             now() AS load_dts,
             md5(ROW(c.first_name, c.last_name, c.date_of_birth,
                     c.residential_state, c.marketing_consent)::text) AS hash_diff,
             c.first_name, c.last_name, c.date_of_birth, c.residential_state,
             c.marketing_consent, c.source_version, c.src_commit_ts
      FROM core_customer c) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_customer_details s
       WHERE s.hk_customer = new.hk_customer ORDER BY s.load_dts DESC LIMIT 1);

-- 5b. Account state (core_banking).
INSERT INTO silver.sat_account_state (hk_account, load_dts, hash_diff, account_status, channel,
                                      opened_at, activated_at, source_version, src_commit_ts,
                                      record_source, run_id)
SELECT new.*, 'core_banking (Kafka)', :run_id
FROM (SELECT md5(a.account_id::text) AS hk_account,
             now() AS load_dts,
             md5(ROW(a.account_status, a.channel, a.opened_at, a.activated_at)::text) AS hash_diff,
             a.account_status, a.channel, a.opened_at, a.activated_at,
             a.source_version, a.src_commit_ts
      FROM core_account a) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_account_state s
       WHERE s.hk_account = new.hk_account ORDER BY s.load_dts DESC LIMIT 1);

-- 5c. Product details (core_banking.product).
INSERT INTO silver.sat_product_details (hk_product, load_dts, hash_diff, product_name, product_type,
                                        record_source, run_id)
SELECT new.*, 'core_banking.product', :run_id
FROM (SELECT md5(p.product_code) AS hk_product,
             now() AS load_dts,
             md5(ROW(p.product_name, p.product_type)::text) AS hash_diff,
             p.product_name, p.product_type
      FROM bronze.core_product p) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_product_details s
       WHERE s.hk_product = new.hk_product ORDER BY s.load_dts DESC LIMIT 1);

-- 5d. KYC and watchlist (identity_fraud), per customer:
--     the latest KYC attempt's result, the number of attempts, and the
--     latest watchlist result.
INSERT INTO silver.sat_customer_kyc (hk_customer, load_dts, hash_diff, kyc_result, kyc_attempts,
                                     watchlist_status, record_source, run_id)
SELECT new.*, 'identity_fraud', :run_id
FROM (SELECT hc.hk_customer,
             now() AS load_dts,
             md5(ROW(k.kyc_result, k.kyc_attempts, w.match_status)::text) AS hash_diff,
             k.kyc_result, k.kyc_attempts, w.match_status
      FROM silver.hub_customer hc
      JOIN (SELECT customer_id,
                   (array_agg(result ORDER BY attempt_no DESC))[1] AS kyc_result,
                   count(*)                                       AS kyc_attempts
            FROM bronze.fraud_kyc_verification
            GROUP BY customer_id) k ON k.customer_id = hc.customer_id
      LEFT JOIN (SELECT DISTINCT ON (customer_id) customer_id, match_status
                 FROM bronze.fraud_watchlist_screening
                 ORDER BY customer_id, screening_id DESC) w ON w.customer_id = hc.customer_id
     ) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_customer_kyc s
       WHERE s.hk_customer = new.hk_customer ORDER BY s.load_dts DESC LIMIT 1);

-- 5e. Risk assessment (identity_fraud), per account.
INSERT INTO silver.sat_account_risk (hk_account, load_dts, hash_diff, risk_score, risk_band, decision,
                                     reason_codes, assessed_at, record_source, run_id)
SELECT new.*, 'identity_fraud', :run_id
FROM (SELECT ha.hk_account,
             now() AS load_dts,
             md5(ROW(r.risk_score, r.risk_band, r.decision, r.reason_codes, r.assessed_at)::text) AS hash_diff,
             r.risk_score, r.risk_band, r.decision, r.reason_codes, r.assessed_at
      FROM silver.hub_account ha
      JOIN (SELECT DISTINCT ON (account_id) *
            FROM bronze.fraud_risk_assessment
            ORDER BY account_id, assessment_id DESC) r ON r.account_id = ha.account_id
     ) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_account_risk s
       WHERE s.hk_account = new.hk_account ORDER BY s.load_dts DESC LIMIT 1);

-- 5f. CRM view of the account (crm): the customer's segment, the status
--     and lifecycle stage CRM holds, and the welcome-call task status.
--     Bronze is an append log, so take the latest copy of each CRM row.
INSERT INTO silver.sat_account_crm (hk_account, load_dts, hash_diff, segment, crm_account_status,
                                    lifecycle_stage, welcome_task_status, record_source, run_id)
SELECT new.*, 'crm', :run_id
FROM (SELECT ha.hk_account,
             now() AS load_dts,
             md5(ROW(ct.segment, fa.account_status, fa.lifecycle_stage, cs.case_status)::text) AS hash_diff,
             ct.segment, fa.account_status, fa.lifecycle_stage, cs.case_status
      FROM silver.hub_account ha
      JOIN (SELECT DISTINCT ON (account_id) *
            FROM bronze.crm_financial_account
            ORDER BY account_id, synced_at DESC) fa ON fa.account_id = ha.account_id
      JOIN (SELECT DISTINCT ON (contact_id) *
            FROM bronze.crm_contact
            ORDER BY contact_id, synced_at DESC) ct ON ct.contact_id = fa.contact_id
      LEFT JOIN (SELECT DISTINCT ON (account_id) *
                 FROM bronze.crm_case
                 WHERE case_type = 'WELCOME_CALL'
                 ORDER BY account_id, case_id DESC) cs ON cs.account_id = ha.account_id
     ) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_account_crm s
       WHERE s.hk_account = new.hk_account ORDER BY s.load_dts DESC LIMIT 1);

-- 5g. Loyalty view of the account (loyalty): member status and tier of the
--     account's member, the welcome offer's points and its redemption.
INSERT INTO silver.sat_account_loyalty (hk_account, load_dts, hash_diff, member_status, tier,
                                        bonus_points, redemption_status, record_source, run_id)
SELECT new.*, 'loyalty', :run_id
FROM (SELECT ha.hk_account,
             now() AS load_dts,
             md5(ROW(m.member_status, m.tier, o.bonus_points, mo.redemption_status)::text) AS hash_diff,
             m.member_status, m.tier, o.bonus_points, mo.redemption_status
      FROM silver.hub_account ha
      JOIN (SELECT DISTINCT ON (account_id) *
            FROM bronze.loyalty_member_offer
            ORDER BY account_id, synced_at DESC) mo ON mo.account_id = ha.account_id
      JOIN (SELECT DISTINCT ON (member_id) *
            FROM bronze.loyalty_member
            ORDER BY member_id, synced_at DESC) m ON m.member_id = mo.member_id
      LEFT JOIN bronze.loyalty_offer o ON o.offer_id = mo.offer_id
     ) AS new
WHERE new.hash_diff IS DISTINCT FROM
      (SELECT s.hash_diff FROM silver.sat_account_loyalty s
       WHERE s.hk_account = new.hk_account ORDER BY s.load_dts DESC LIMIT 1);


-- #############################################################################
-- 6. Close the run: rows added to silver by this run, and rows quarantined.
-- #############################################################################

CREATE TEMP TABLE loaded ON COMMIT DROP AS
          SELECT 'silver.hub_customer'         AS silver_table, count(*) AS rows_added FROM silver.hub_customer         WHERE run_id = :run_id
UNION ALL SELECT 'silver.hub_account',           count(*) FROM silver.hub_account           WHERE run_id = :run_id
UNION ALL SELECT 'silver.hub_product',           count(*) FROM silver.hub_product           WHERE run_id = :run_id
UNION ALL SELECT 'silver.hub_device',            count(*) FROM silver.hub_device            WHERE run_id = :run_id
UNION ALL SELECT 'silver.link_account_holder',   count(*) FROM silver.link_account_holder   WHERE run_id = :run_id
UNION ALL SELECT 'silver.link_account_product',  count(*) FROM silver.link_account_product  WHERE run_id = :run_id
UNION ALL SELECT 'silver.link_customer_device',  count(*) FROM silver.link_customer_device  WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_customer_details',  count(*) FROM silver.sat_customer_details  WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_account_state',     count(*) FROM silver.sat_account_state     WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_product_details',   count(*) FROM silver.sat_product_details   WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_customer_kyc',      count(*) FROM silver.sat_customer_kyc      WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_account_risk',      count(*) FROM silver.sat_account_risk      WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_account_crm',       count(*) FROM silver.sat_account_crm       WHERE run_id = :run_id
UNION ALL SELECT 'silver.sat_account_loyalty',   count(*) FROM silver.sat_account_loyalty   WHERE run_id = :run_id
UNION ALL SELECT 'ops.dq_quarantine (rejected)', count(*) FROM ops.dq_quarantine            WHERE run_id = :run_id;

UPDATE ops.etl_run
   SET finished_at   = now(),
       status        = 'SUCCEEDED',
       rows_loaded   = (SELECT sum(rows_added) FROM loaded WHERE silver_table LIKE 'silver.%'),
       rows_rejected = (SELECT count(*) FROM ops.dq_quarantine WHERE run_id = :run_id),
       -- read = loaded + rejected (unchanged rows are skipped, not counted)
       rows_read     = (SELECT sum(rows_added) FROM loaded)
 WHERE run_id = :run_id;

SELECT * FROM loaded;

COMMIT;
