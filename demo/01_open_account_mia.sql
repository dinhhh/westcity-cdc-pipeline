-- Scene 1: Mia opens an EVERYDAY account (expected: APPROVE, then ACTIVE,
-- CRM welcome call, loyalty BRONZE + 5,000 points).
-- Run: docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/01_open_account_mia.sql

\set ON_ERROR_STOP on
-- Hide psql noise
\set QUIET on
\pset footer off

-- 1. Reserve customer and account ids
\connect core_banking
SELECT nextval(pg_get_serial_sequence('customer', 'customer_id')) AS customer_id,
       nextval(pg_get_serial_sequence('account',  'account_id'))  AS account_id \gset


-- 2. identity_fraud checks (before the account opens)
\connect identity_fraud

-- Her own new phone
INSERT INTO device (device_fingerprint)
VALUES (md5('mia-phone-' || :customer_id))
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
VALUES (:customer_id, 'Mia', 'Nguyen',
        current_date - interval '27 years 3 months',                     -- 27 years old
        'mia.nguyen' || :customer_id || '@example.com', '+61412345678',
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


-- 4. Show the result
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
