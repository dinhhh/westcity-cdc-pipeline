-- =============================================================================
-- File:     sql/01_create_databases.sql
-- Purpose:  Create the five databases of the West City Bank prototype.
-- Runs:     Automatically, once, when the Postgres container starts with an
--           empty data volume (docker-entrypoint-initdb.d), as westcity_admin.
--
-- Design map (solution architecture):
--   core_banking    source system of record: customers, accounts, outbox
--   identity_fraud  source: identity verification and fraud platform
--   crm             source: customer relationship management
--   loyalty         source: rewards and loyalty platform
--   westcity_dw     integrated data warehouse (staging, raw_vault, mart, ops)
-- All five share one PostgreSQL container: a stated lab simplification.
-- In production each system would have its own server.

CREATE DATABASE core_banking   ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE identity_fraud ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE crm            ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE loyalty        ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE westcity_dw    ENCODING 'UTF8' TEMPLATE template0;

-- 2. Business time zone. Timestamps are stored as timestamptz (absolute
--    moments in time); 

ALTER DATABASE core_banking   SET timezone = 'Australia/Sydney';
ALTER DATABASE identity_fraud SET timezone = 'Australia/Sydney';
ALTER DATABASE crm            SET timezone = 'Australia/Sydney';
ALTER DATABASE loyalty        SET timezone = 'Australia/Sydney';
ALTER DATABASE westcity_dw    SET timezone = 'Australia/Sydney';


-- 3. Descriptions, visible with \l+ in psql.
COMMENT ON DATABASE core_banking   IS 'Core banking: system of record for customers and accounts; publishes events via the outbox';
COMMENT ON DATABASE identity_fraud IS 'Identity verification and fraud: devices, onboarding sessions, KYC, watchlist screening, risk assessments';
COMMENT ON DATABASE crm            IS 'CRM: contacts, financial accounts and cases, synced from core_banking events';
COMMENT ON DATABASE loyalty        IS 'Loyalty: members, account links and offers, synced from core_banking events';
COMMENT ON DATABASE westcity_dw    IS 'Data warehouse: staging -> raw_vault (Data Vault 2.0) -> mart (star schema), plus ops (audit and reconciliation)';
