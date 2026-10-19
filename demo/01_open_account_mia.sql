-- =============================================================================
-- Mary: 27 years old, lives in Parramatta NSW, gives marketing consent,
-- signs up on her own new phone, passes KYC, clear on the watchlist.
-- Expected:
--   fraud      APPROVE, LOW score (no risk signals, only the random base 0-25)
--   after demo/apply_decisions.sql:
--   core       ACTIVE
--   CRM        segment YOUNG_PROFESSIONAL, stage NEW_ONBOARDING, welcome-call task
--   loyalty    enrolled, BRONZE, 5,000 welcome points (EVERYDAY)
-- Running it again opens another account for another "Mary" (new ids).
-- =============================================================================

\set ON_ERROR_STOP on
-- Quiet: no "INSERT 0 1" / "connected to" lines, only the results.
\set QUIET on
\pset footer off

-- 1. Take Mary's customer and account ids from core banking's counters
--    (the app reserves them when she starts signing up).
\connect core_banking
SELECT nextval(pg_get_serial_sequence('customer', 'customer_id')) AS customer_id,
       nextval(pg_get_serial_sequence('account',  'account_id'))  AS account_id \gset


-- 2. identity_fraud: the app's identity checks, done BEFORE the account is
--    opened, so the fraud consumer finds them when AccountOpened arrives.
\connect identity_fraud

-- Her own, brand-new phone (a device no one else has used).
INSERT INTO device (device_fingerprint)
VALUES (md5('Mary-phone-' || :customer_id))
RETURNING device_id \gset

INSERT INTO onboarding_session (session_id, customer_id, device_id, started_at)
VALUES (md5('session-' || :customer_id)::uuid, :customer_id, :device_id,
        now() - interval '6 minutes');                                   -- sign-up took 6 minutes

INSERT INTO kyc_verification (customer_id, attempt_no, result)
VALUES (:customer_id, 1, 'PASS');

INSERT INTO watchlist_screening (customer_id, match_status)
VALUES (:customer_id, 'CLEAR');


-- 3. core_banking: open the account in ONE transaction. At COMMIT the
--    triggers (sql/03) write CustomerCreated and AccountOpened to the outbox.
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
