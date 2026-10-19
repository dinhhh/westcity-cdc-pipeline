-- -----------------------------------------------------------------------------
-- 1. Databases. UTF8 encoding; template0 gives a clean, predictable copy.
-- -----------------------------------------------------------------------------
CREATE DATABASE core_banking   ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE identity_fraud ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE crm            ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE loyalty        ENCODING 'UTF8' TEMPLATE template0;
CREATE DATABASE westcity_dw    ENCODING 'UTF8' TEMPLATE template0;

-- Default time zone: Sydney

ALTER DATABASE core_banking   SET timezone = 'Australia/Sydney';
ALTER DATABASE identity_fraud SET timezone = 'Australia/Sydney';
ALTER DATABASE crm            SET timezone = 'Australia/Sydney';
ALTER DATABASE loyalty        SET timezone = 'Australia/Sydney';
ALTER DATABASE westcity_dw    SET timezone = 'Australia/Sydney';


-- Descriptions (see \l+)
COMMENT ON DATABASE core_banking   IS 'Core banking: system of record for customers and accounts; publishes events via the outbox';
COMMENT ON DATABASE identity_fraud IS 'Identity verification and fraud: devices, onboarding sessions, KYC, watchlist screening, risk assessments';
COMMENT ON DATABASE crm            IS 'CRM: contacts, financial accounts and cases, synced from core_banking events';
COMMENT ON DATABASE loyalty        IS 'Loyalty: members, account links and offers, synced from core_banking events';
COMMENT ON DATABASE westcity_dw    IS 'Data warehouse: staging -> raw_vault (Data Vault 2.0) -> mart (star schema), plus ops (audit and reconciliation)';
