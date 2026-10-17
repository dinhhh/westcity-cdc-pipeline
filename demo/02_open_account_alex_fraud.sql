-- Scene 5: fraudster Alex uses a fraud-ring device and fails KYC
-- (expected: DECLINE, then REJECTED, no CRM task, not enrolled in loyalty).
-- Needs 2+ history customers on the ring device.
-- Run: docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/02_open_account_alex_fraud.sql

\set ON_ERROR_STOP on
-- Hide psql noise
\set QUIET on
\pset footer off

-- 1. Reserve customer and account ids
\connect core_banking
SELECT nextval(pg_get_serial_sequence('customer', 'customer_id')) AS customer_id,
       nextval(pg_get_serial_sequence('account',  'account_id'))  AS account_id \gset


-- 2. identity_fraud checks
\connect identity_fraud

-- Busiest fraud-ring device
SELECT d.device_id                     AS ring_device_id,
       count(DISTINCT s.customer_id)   AS already_used_by
FROM device d
LEFT JOIN onboarding_session s ON s.device_id = d.device_id
WHERE d.device_fingerprint IN (md5('ring-device-1'), md5('ring-device-2'),
                               md5('ring-device-3'), md5('ring-device-4'))
GROUP BY d.device_id
ORDER BY already_used_by DESC, d.device_id
LIMIT 1 \gset

\echo '=== Alex signs up on fraud-ring device' :ring_device_id 'which' :already_used_by 'other customers already used.'
SELECT :already_used_by >= 2 AS ring_is_big_enough \gset
\if :ring_is_big_enough
\else
    \echo 'NOTE: fewer than 2 other customers on that device, so SHARED_DEVICE will not'
    \echo '      fire (it needs 3+ incl. Alex). Seed more history (seed_count 2000).'
\endif

INSERT INTO onboarding_session (session_id, customer_id, device_id, started_at)
VALUES (md5('session-' || :customer_id)::uuid, :customer_id, :ring_device_id,
        now() - interval '4 minutes');

INSERT INTO kyc_verification (customer_id, attempt_no, result)
VALUES (:customer_id, 1, 'FAIL');                                        -- KYC failed

INSERT INTO watchlist_screening (customer_id, match_status)
VALUES (:customer_id, 'CLEAR');


-- 3. Open the account in one transaction
\connect core_banking

BEGIN;

INSERT INTO customer (customer_id, first_name, last_name, date_of_birth, email, mobile,
                      marketing_consent)
VALUES (:customer_id, 'Alex', 'Morgan',
        current_date - interval '31 years 7 months',
        'alex.morgan' || :customer_id || '@example.com', '+61498765432',
        true);

INSERT INTO customer_address (customer_id, address_type, line1, suburb, state, postcode)
VALUES (:customer_id, 'RESIDENTIAL', '88 High Street', 'Southport', 'QLD', '4215');

INSERT INTO account (account_id, branch_id, account_number, product_code, channel,
                     onboarding_session_id)
VALUES (:account_id,
        (SELECT branch_id FROM branch WHERE bsb = '934000'),             -- Brisbane CBD
        (10000000 + :account_id)::text,
        'EVERYDAY', 'MOBILE',
        md5('session-' || :customer_id)::uuid);

INSERT INTO account_holder (account_id, customer_id, holder_role)
VALUES (:account_id, :customer_id, 'PRIMARY');

COMMIT;


-- 4. Show the result
\echo
\echo '=== Alex''s account is open, PENDING until the fraud decision is applied:'
SELECT a.account_id, c.first_name || ' ' || c.last_name AS customer,
       a.product_code AS product, a.channel, a.account_status, a.opened_at
FROM account a
JOIN customer c ON c.customer_id = :customer_id
WHERE a.account_id = :account_id;

\echo '=== Events written to the outbox in the same transaction (now on their way to Kafka):'
SELECT type AS event_type, aggregateid AS kafka_key, event_version, created_at AS written_at
FROM outbox
WHERE (aggregatetype = 'account'  AND aggregateid = :'account_id')
   OR (aggregatetype = 'customer' AND aggregateid = :'customer_id')
ORDER BY created_at, type;

\echo 'Next: follow it with'
\echo '  docker compose exec -T postgres psql -U westcity_admin -d core_banking -v account_id=':account_id' < demo/03_trace_account.sql'
