-- =============================================================================
-- File:     demo/01_open_account_mia.sql
-- Purpose:  Demo scene 1: Mia Nguyen opens an EVERYDAY account in the mobile
--           app (docs/demo-flow.md section 7).
--           Like the real app, the script first records her identity checks
--           in identity_fraud, then opens the account in core_banking. From
--           there the account travels the real pipeline:
--             core_banking.outbox -> Debezium -> Kafka
--               -> consumer-fraud, consumer-crm, consumer-loyalty, consumer-dw
-- Run:      docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/01_open_account_mia.sql
--
-- Mia: 27 years old, lives in Parramatta NSW, gives marketing consent,
-- signs up on her own new phone, passes KYC, clear on the watchlist.
-- Expected:
--   fraud      APPROVE, LOW score (no risk signals, only the random base 0-25)
--   after demo/apply_decisions.sql:
--   core       ACTIVE
--   CRM        segment YOUNG_PROFESSIONAL, stage NEW_ONBOARDING, welcome-call task
--   loyalty    enrolled, BRONZE, 5,000 welcome points (EVERYDAY)
-- Running it again opens another account for another "Mia" (new ids).
-- =============================================================================

\set ON_ERROR_STOP on
-- Hide psql noise
\set QUIET on
\pset footer off

-- 1. Take Mia's customer and account ids from core banking's counters
--    (the app reserves them when she starts signing up).
\connect core_banking
SELECT nextval(pg_get_serial_sequence('customer', 'customer_id')) AS customer_id,
       nextval(pg_get_serial_sequence('account',  'account_id'))  AS account_id \gset


-- 2. identity_fraud checks (before the account opens)
\connect identity_fraud

-- Her own new phone
INSERT INTO device (device_fingerprint)
VALUES (md5('Mary-phone-' || :customer_id))
RETURNING device_id \gset

INSERT INTO onboarding_session (session_id, customer_id, device_id, started_at)
VALUES (md5('session-' || :customer_id)::uuid, :customer_id, :device_id,
        now() - interval '6 minutes');                                   -- 6-minute sign-up

INSERT INTO kyc_verification (customer_id, attempt_no, result)
VALUES (:customer_id, 1, 'PASS');

INSERT INTO watchlist_screening (customer_id, match_status)
VALUES (:customer_id, 'CLEAR');


-- 3. Open the account in one transaction (outbox events at COMMIT)
\connect core_banking

BEGIN;

INSERT INTO customer (customer_id, first_name, last_name, date_of_birth, email, mobile,
                      marketing_consent)
VALUES (:customer_id, 'Mary', 'Nguyen',
        current_date - interval '27 years 3 months',                     -- 27 years old
        'mary.nguyen' || :customer_id || '@example.com', '+61412345678',
        true);

INSERT INTO customer_address (customer_id, address_type, line1, suburb, state, postcode)
VALUES (:customer_id, 'RESIDENTIAL', '12 Church Street', 'Parramatta', 'NSW', '2150');

INSERT INTO account (account_id, branch_id, account_number, product_code, channel,
                     onboarding_session_id)
VALUES (:account_id,
        (SELECT branch_id FROM branch WHERE bsb = '932150'),             -- Parramatta branch
        (10000000 + :account_id)::text,
        'EVERYDAY', 'MOBILE',
        md5('session-' || :customer_id)::uuid);                          -- links to identity_fraud

INSERT INTO account_holder (account_id, customer_id, holder_role)
VALUES (:account_id, :customer_id, 'PRIMARY');

COMMIT;


-- 4. Show what happened in core banking.
\echo
\echo '=== Mia''s account is open. It stays PENDING until the fraud decision is applied:'
SELECT a.account_id, c.first_name || ' ' || c.last_name AS customer,
       a.product_code AS product, a.channel, a.account_status, a.opened_at
FROM account a
JOIN customer c ON c.customer_id = :customer_id
WHERE a.account_id = :account_id;

\echo '=== In the SAME transaction the triggers wrote these events to the outbox.'
\echo '    Debezium reads them from the database log and sends them to Kafka:'
SELECT type AS event_type, aggregateid AS kafka_key, event_version, created_at AS written_at
FROM outbox
WHERE (aggregatetype = 'account'  AND aggregateid = :'account_id')
   OR (aggregatetype = 'customer' AND aggregateid = :'customer_id')
ORDER BY created_at, type;

\echo 'Next: follow it with'
\echo '  docker compose exec -T postgres psql -U westcity_admin -d core_banking -v account_id=':account_id' < demo/03_trace_account.sql'
