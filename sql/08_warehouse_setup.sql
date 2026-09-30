-- =============================================================================
-- File:     sql/08_warehouse_setup.sql
-- Purpose:  Connect the warehouse (westcity_dw) to its sources:
--             1. read-only links to the source databases (postgres_fdw),
--                used by etl/01_extract_bronze.sql
--             2. the starting watermark of every incrementally extracted table
--             3. bronze.land_core_event(): how consumer-dw stores a Kafka
--                event in bronze.core_event
-- Runs:     Automatically after 07_reference_data.sql on an empty data volume.
--
-- How each source reaches the warehouse:
--   core_banking    through Kafka: consumer-dw lands every customer / account
--                   event in bronze.core_event (+ a small SQL copy of the
--                   product table, for product names)
--   identity_fraud  SQL extract (incremental, watermarks)
--   crm             SQL extract (incremental, watermarks)
--   loyalty         SQL extract (incremental, watermarks)
--   (Fraud, CRM and Loyalty results are not published to Kafka; only
--   core_banking has an outbox.)
-- =============================================================================

\connect westcity_dw


-- #############################################################################
-- 1. Read-only links to the source databases (postgres_fdw).
--    For each source: a server (where it is), a user mapping (how to log
--    in), and a schema named after the database holding "foreign tables":
--    local names for the remote tables. So in westcity_dw,
--    crm.crm_case means "the crm_case table in the crm database".
--    Only the tables the extract needs are imported.
-- #############################################################################
CREATE EXTENSION postgres_fdw;

-- core_banking: only the product table (customers and accounts arrive
-- through Kafka).
CREATE SERVER core_banking_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'core_banking');
CREATE USER MAPPING FOR westcity_admin SERVER core_banking_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA core_banking;
IMPORT FOREIGN SCHEMA public LIMIT TO (product)
    FROM SERVER core_banking_server INTO core_banking;

-- identity_fraud
CREATE SERVER identity_fraud_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'identity_fraud');
CREATE USER MAPPING FOR westcity_admin SERVER identity_fraud_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA identity_fraud;
IMPORT FOREIGN SCHEMA public
    LIMIT TO (device, onboarding_session, kyc_verification, watchlist_screening, risk_assessment)
    FROM SERVER identity_fraud_server INTO identity_fraud;

-- crm
CREATE SERVER crm_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'crm');
CREATE USER MAPPING FOR westcity_admin SERVER crm_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA crm;
IMPORT FOREIGN SCHEMA public LIMIT TO (crm_contact, crm_financial_account, crm_case)
    FROM SERVER crm_server INTO crm;

-- loyalty
CREATE SERVER loyalty_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'loyalty');
CREATE USER MAPPING FOR westcity_admin SERVER loyalty_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA loyalty;
IMPORT FOREIGN SCHEMA public LIMIT TO (loyalty_offer, loyalty_member, member_offer)
    FROM SERVER loyalty_server INTO loyalty;


-- #############################################################################
-- 2. Starting watermarks: nothing extracted yet (id 0, time -infinity).
--    Insert-only tables use an id that only grows; tables whose rows are
--    UPDATED use synced_at, which the consumer sets on every write.
--    Not listed (copied in full each run): core_banking.product,
--    loyalty.loyalty_offer (small reference tables) and
--    identity_fraud.onboarding_session (its only time, started_at, is
--    backdated for the history, so it cannot act as a watermark).
-- #############################################################################
INSERT INTO ops.extract_watermark (source_table, watermark_column, last_id, last_ts) VALUES
    ('identity_fraud.device',              'device_id',     0, NULL),
    ('identity_fraud.kyc_verification',    'kyc_id',        0, NULL),
    ('identity_fraud.watchlist_screening', 'screening_id',  0, NULL),
    ('identity_fraud.risk_assessment',     'assessment_id', 0, NULL),
    ('crm.crm_contact',                    'synced_at',     NULL, '-infinity'),
    ('crm.crm_financial_account',          'synced_at',     NULL, '-infinity'),
    ('crm.crm_case',                       'case_id',       0, NULL),
    ('loyalty.loyalty_member',             'synced_at',     NULL, '-infinity'),
    ('loyalty.member_offer',               'synced_at',     NULL, '-infinity');


-- #############################################################################
-- 3. bronze.land_core_event(): called by consumer-dw (consumers/consumer.py dw)
--    for every event it reads from bank.account.events and
--    bank.customer.events. It stores the event unchanged, plus where it was
--    read in Kafka (lineage). The event id is the primary key, so a repeat
--    delivery is skipped (idempotent consumer).
-- #############################################################################
CREATE FUNCTION bronze.land_core_event(
    p_event     jsonb,     -- the whole event, as read from Kafka
    p_topic     text,      -- Kafka topic
    p_partition integer,   -- Kafka partition
    p_offset    bigint)    -- position in that partition
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bronze.core_event (event_id, event_type, aggregate_type, aggregate_id,
                                   aggregate_version, occurred_at, payload,
                                   kafka_topic, kafka_partition, kafka_offset)
    VALUES ((p_event ->> 'event_id')::uuid,
            p_event ->> 'event_type',
            p_event ->> 'aggregate_type',
            (p_event ->> 'aggregate_id')::bigint,
            (p_event ->> 'aggregate_version')::integer,
            (p_event ->> 'occurred_at')::timestamptz,
            p_event,
            p_topic, p_partition, p_offset)
    ON CONFLICT (event_id) DO NOTHING;

    IF NOT FOUND THEN
        RETURN 'skipped: event already landed';
    END IF;
    RETURN 'landed in bronze.core_event';
END;
$$;

COMMENT ON FUNCTION bronze.land_core_event IS
    'Store one core_banking Kafka event in bronze.core_event (idempotent by event_id)';
