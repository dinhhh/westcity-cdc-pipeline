-- Warehouse setup: FDW links to the sources, starting watermarks, and
-- bronze.land_core_event() for consumer-dw. core_banking arrives via Kafka,
-- the other sources are extracted with SQL.

\connect westcity_dw


-- 1. FDW links to source databases (only the needed tables)
CREATE EXTENSION postgres_fdw;

-- core_banking: product only (the rest comes via Kafka)
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


-- 2. Starting watermarks. Reference tables and onboarding_session are
-- copied in full each run instead.
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


-- 3. Land a Kafka event in bronze (repeat event ids are skipped)
CREATE FUNCTION bronze.land_core_event(
    p_event     jsonb,     -- full event
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
