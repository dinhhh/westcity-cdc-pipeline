-- =============================================================================
-- File:     demo/03_trace_account.sql
-- Purpose:  Demo scene 2: follow ONE account through every system, in the
--           order the data travels (docs/demo-flow.md section 7):
--             1. core banking   the account + the events in its outbox
--             2. Kafka          topic / partition / offset of each event
--             3. identity & fraud  signals, score, decision
--             4. CRM            segment, lifecycle stage, welcome-call task
--             5. loyalty        membership, tier, welcome offer
--             6. warehouse      silver history + the gold fact row
--           Each system also shows WHICH events it processed and how many
--           seconds after they were written to the outbox (the
--           processed_event table the consumers keep for idempotency).
-- Run:      the newest account:
--             docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/03_trace_account.sql
--           a given account:
--             docker compose exec -T postgres psql -U westcity_admin -d core_banking -v account_id=2011 < demo/03_trace_account.sql
--           Read-only: safe to run as often as you like, at any stage.
-- =============================================================================

\set ON_ERROR_STOP on
-- Quiet: no "INSERT 0 1" / "connected to" lines, only the results.
\set QUIET on
\pset null '-'
\pset footer off

\connect core_banking

-- Which account: the one given with -v account_id=..., else the newest.
\if :{?account_id}
\else
    SELECT max(account_id) AS account_id FROM account \gset
\endif
SELECT customer_id FROM account_holder
WHERE account_id = :account_id AND holder_role = 'PRIMARY' \gset

-- Remember this account's outbox events (id, type, time written) in one
-- psql variable, as a VALUES list, to follow them into the other systems.
SELECT string_agg(format('(%L::uuid, %L, %L::timestamptz)', id, type, created_at),
                  ', ' ORDER BY created_at, type) AS events
FROM outbox
WHERE (aggregatetype = 'account'  AND aggregateid = :'account_id')
   OR (aggregatetype = 'customer' AND aggregateid = :'customer_id') \gset


-- #############################################################################
\echo
\echo '############ 1. CORE BANKING (system of record) ############'
SELECT a.account_id, c.first_name || ' ' || c.last_name AS customer,
       a.product_code AS product, a.channel, a.account_status, a.version,
       a.opened_at, a.activated_at
FROM account a
JOIN customer c ON c.customer_id = :customer_id
WHERE a.account_id = :account_id;

\echo '--- events written to the outbox (same transaction as each change):'
SELECT e.type AS event_type, e.written_at
FROM (VALUES :events) AS e (event_id, type, written_at)
ORDER BY e.written_at, e.type;


-- #############################################################################
\connect westcity_dw
\echo
\echo '############ 2. KAFKA (Debezium published each event; consumer-dw recorded where it read it) ############'
SELECT e.type AS event_type,
       b.kafka_topic, b.kafka_partition, b.kafka_offset,
       round(extract(epoch FROM b.landed_at - e.written_at)::numeric, 2) AS seconds_to_warehouse_bronze
FROM (VALUES :events) AS e (event_id, type, written_at)
LEFT JOIN bronze.core_event b ON b.event_id = e.event_id
ORDER BY e.written_at, e.type;


-- #############################################################################
\connect identity_fraud
\echo
\echo '############ 3. IDENTITY & FRAUD ############'
\echo '--- events the fraud consumer processed (it only needs AccountOpened):'
SELECT e.type AS event_type,
       round(extract(epoch FROM p.processed_at - e.written_at)::numeric, 2) AS seconds_after_written
FROM (VALUES :events) AS e (event_id, type, written_at)
LEFT JOIN processed_event p ON p.event_id = e.event_id
ORDER BY e.written_at, e.type;

\echo '--- the signals it checked, and its decision:'
SELECT (SELECT count(DISTINCT other.customer_id)
        FROM onboarding_session s
        JOIN onboarding_session other ON other.device_id = s.device_id
        WHERE s.customer_id = :customer_id)                           AS customers_on_device,
       (SELECT result FROM kyc_verification
        WHERE customer_id = :customer_id
        ORDER BY attempt_no DESC LIMIT 1)                             AS kyc_result,
       (SELECT match_status FROM watchlist_screening
        WHERE customer_id = :customer_id)                             AS watchlist,
       r.risk_score, r.risk_band, r.decision, r.reason_codes, r.assessed_at
FROM (SELECT 1) AS one
LEFT JOIN risk_assessment r ON r.account_id = :account_id;


-- #############################################################################
\connect crm
\echo
\echo '############ 4. CRM ############'
\echo '--- events the CRM consumer processed:'
SELECT e.type AS event_type,
       round(extract(epoch FROM p.processed_at - e.written_at)::numeric, 2) AS seconds_after_written
FROM (VALUES :events) AS e (event_id, type, written_at)
LEFT JOIN processed_event p ON p.event_id = e.event_id
ORDER BY e.written_at, e.type;

\echo '--- the customer and account as CRM holds them:'
SELECT ct.segment, fa.account_status, fa.lifecycle_stage, fa.source_version AS account_version,
       cs.case_type AS task, cs.case_status AS task_status, cs.opened_at AS task_opened_at
FROM (SELECT 1) AS one
LEFT JOIN crm_financial_account fa ON fa.account_id = :account_id
LEFT JOIN crm_contact ct           ON ct.contact_id = fa.contact_id
LEFT JOIN crm_case cs              ON cs.account_id = fa.account_id;


-- #############################################################################
\connect loyalty
\echo
\echo '############ 5. LOYALTY ############'
\echo '--- events the loyalty consumer processed (it needs AccountActivated / AccountRejected):'
SELECT e.type AS event_type,
       round(extract(epoch FROM p.processed_at - e.written_at)::numeric, 2) AS seconds_after_written
FROM (VALUES :events) AS e (event_id, type, written_at)
LEFT JOIN processed_event p ON p.event_id = e.event_id
ORDER BY e.written_at, e.type;

\echo '--- membership and welcome offer (all "-" = not enrolled):'
SELECT m.member_status, m.tier, o.bonus_points AS welcome_points,
       mo.redemption_status, mo.assigned_at
FROM (SELECT 1) AS one
LEFT JOIN member_offer mo   ON mo.account_id = :account_id
LEFT JOIN loyalty_member m  ON m.member_id = mo.member_id
LEFT JOIN loyalty_offer o   ON o.offer_id = mo.offer_id;


-- #############################################################################
\connect westcity_dw
\echo
\echo '############ 6. DATA WAREHOUSE (filled by etl/run_all.sql) ############'
\echo '--- silver: history of the account status (one row per change, never overwritten; empty = not loaded yet):'
SELECT s.load_dts AS loaded_at, s.account_status, s.source_version AS account_version, s.run_id
FROM silver.hub_account h
JOIN silver.sat_account_state s ON s.hk_account = h.hk_account
WHERE h.account_id = :account_id
ORDER BY s.load_dts;

\echo '--- gold: the ONE combined row the dashboards read (all "-" = not loaded yet: run etl/run_all.sql):'
SELECT f.account_id, cu.customer_name, f.account_status, f.risk_decision, f.risk_score,
       f.segment, f.lifecycle_stage, f.loyalty_enrolled, f.welcome_offer_points,
       f.run_id, r.finished_at AS loaded_at
FROM (SELECT 1) AS one
LEFT JOIN gold.fact_account_opening f ON f.account_id = :account_id
LEFT JOIN gold.dim_customer cu        ON cu.customer_key = f.customer_key
LEFT JOIN ops.etl_run r               ON r.run_id = f.run_id;
