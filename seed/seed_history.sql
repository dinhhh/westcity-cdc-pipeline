-- =============================================================================
-- File:     seed/seed_history.sql
-- Purpose:  Create the synthetic history: :seed_count new customers who each
--           open one account on a random day in the last 90 days. The
--           accounts go through the REAL pipeline, like the live demo:
--             identity_fraud rows -> core_banking insert -> outbox ->
--             Debezium -> Kafka -> fraud / CRM / loyalty consumers
--
-- Run it AFTER `docker compose up -d --build`, once everything is running:
--   1. docker compose exec -T postgres psql -U westcity_admin -d core_banking < seed/seed_history.sql
--   2. wait until the fraud consumer has scored the new accounts
--        docker compose logs -f consumer-fraud
--   3. apply the decisions (APPROVE -> ACTIVE, DECLINE -> REJECTED):
--        docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql
--   Running it again ADDS another :seed_count customers (new ids).
--
-- How the random data is made: every field is a random pick from a
-- HARD-CODED LIST (names, ages, suburbs, products, ...). A list can carry
-- weights, which is how the percentages of docs/demo-flow.md 2.1 and 2.5
-- are met, e.g. channel MOBILE 70 / WEB 20 / BRANCH 10.
--
-- The script plays the MOBILE APP: the app writes the identity checks
-- (device, session, KYC, watchlist) first, then asks core banking to open
-- the account. It spans two databases, so it runs in psql (\connect,
-- \gset), not in CloudBeaver.
-- =============================================================================

\set ON_ERROR_STOP on

-- HOW MANY customers / accounts to create. Increase this for more history
-- (the spec asks for about 2,000).
\set seed_count 10


-- #############################################################################
-- 1. Generate the new customers (in core_banking, where the ids come from)
-- #############################################################################
\connect core_banking

-- Same data on every fresh rebuild (docs/demo-flow.md 3).
SELECT setseed(0.42);

-- pick(choices, weights): returns one of the choices at random.
--   pg_temp.pick(ARRAY['A','B','C'])                 all equally likely
--   pg_temp.pick(ARRAY['A','B','C'], ARRAY[70,20,10]) A 70%, B 20%, C 10%
-- pg_temp = the function only exists for this connection, so the seed
-- leaves nothing behind in core_banking.
CREATE FUNCTION pg_temp.pick(choices text[], weights integer[] DEFAULT NULL)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    ticket numeric;
BEGIN
    IF weights IS NULL THEN
        RETURN choices[1 + floor(random() * cardinality(choices))::int];
    END IF;
    -- Draw a number between 0 and the sum of the weights, then walk the
    -- list: each choice owns a stretch as long as its weight.
    ticket := random() * (SELECT sum(w) FROM unnest(weights) AS w);
    FOR i IN 1 .. cardinality(choices) LOOP
        ticket := ticket - weights[i];
        IF ticket < 0 THEN
            RETURN choices[i];
        END IF;
    END LOOP;
END;
$$;

-- One row per new customer, every field picked from a hard-coded list.
CREATE TEMP TABLE new_customer AS
WITH day_list AS (
    -- The last 90 days (up to yesterday) and how likely each one is:
    -- weight 100 on the first day rising to 130 on the last (about 25%
    -- more openings per week by the end), times 0.6 at weekends.
    SELECT array_agg(d::date::text ORDER BY d) AS days,
           array_agg(round((100 + 30 * (d::date - (current_date - 90)) / 89.0)
                           * CASE WHEN extract(isodow FROM d) >= 6 THEN 0.6 ELSE 1 END)::int
                     ORDER BY d) AS weights
    FROM generate_series(current_date - 90, current_date - 1, interval '1 day') AS d
),
picked AS (
    SELECT
        -- new ids from core_banking's own counters
        nextval(pg_get_serial_sequence('customer', 'customer_id')) AS customer_id,
        nextval(pg_get_serial_sequence('account',  'account_id'))  AS account_id,

        -- who (no rule in the spec: plausible Australian names)
        pg_temp.pick(ARRAY['Olivia','Liam','Charlotte','Noah','Amelia','Jack','Isla','William',
                           'Mia','Oliver','Grace','Leo','Chloe','Ethan','Priya','Wei',
                           'Minh','Aisha','Arjun','Sofia']) AS first_name,
        pg_temp.pick(ARRAY['Smith','Jones','Williams','Brown','Wilson','Taylor','Nguyen','Lee',
                           'Walker','Harris','Kelly','King','Patel','Chen','Wang','Tran',
                           'Singh','Kim','Murphy','Pham']) AS last_name,
        -- age 18-75, weighted toward 23-45 (16 of these 25 ages)
        pg_temp.pick(ARRAY['18','19','21','22','24',
                           '23','25','26','27','28','29','30','31','32','33','34','36','38','40','42','45',
                           '48','55','62','71'])::int AS age,
        -- where: suburb | state | postcode | home branch BSB. Weights give the
        -- state mix NSW 32, VIC 26, QLD 20, WA 10, SA 7, TAS 2 + ACT 2 + NT 1
        pg_temp.pick(ARRAY['Parramatta|NSW|2150|932150', 'Chatswood|NSW|2067|932000',
                           'Richmond|VIC|3121|933000',   'Box Hill|VIC|3128|933000',
                           'Southport|QLD|4215|934000',  'Toowong|QLD|4066|934000',
                           'Fremantle|WA|6160|936000',   'Subiaco|WA|6008|936000',
                           'Glenelg|SA|5045|935000',     'Sandy Bay|TAS|7005|937000',
                           'Belconnen|ACT|2617|932600',  'Palmerston|NT|0830|935800'],
                     ARRAY[16, 16, 13, 13, 10, 10, 5, 5, 7, 2, 2, 1]) AS home,
        pg_temp.pick(ARRAY['George Street','King Street','Church Street','High Street','Park Road']) AS street,
        pg_temp.pick(ARRAY['true','false'], ARRAY[70, 30])::boolean AS marketing_consent,
        pg_temp.pick(ARRAY['EMAIL','PAPER'], ARRAY[85, 15])        AS statement_delivery,

        -- what and when
        pg_temp.pick(ARRAY['MOBILE','WEB','BRANCH'], ARRAY[70, 20, 10])        AS channel,
        pg_temp.pick(ARRAY['EVERYDAY','SAVINGS','PREMIUM'], ARRAY[55, 30, 5]) AS product,
        pg_temp.pick(l.days, l.weights)::date                                  AS open_date,

        -- identity checks (docs/demo-flow.md 2.1). Device: most customers use
        -- their own; about 1.3% sign up on one of the 4 fraud-ring devices.
        pg_temp.pick(ARRAY['own','ring-device-1','ring-device-2','ring-device-3','ring-device-4'],
                     ARRAY[987, 3, 3, 3, 4])                                   AS device,
        pg_temp.pick(ARRAY['PASS','REFER','FAIL'], ARRAY[90, 7, 3])            AS kyc_result,
        pg_temp.pick(ARRAY['yes','no'], ARRAY[40, 60])                         AS kyc_retry,  -- REFER only: 40% try again
        pg_temp.pick(ARRAY['PASS','REFER'], ARRAY[75, 25])                     AS kyc_retry_result,  -- no rule: plausible
        pg_temp.pick(ARRAY['CLEAR','POSSIBLE','CONFIRMED'], ARRAY[980, 18, 2]) AS watchlist
    FROM generate_series(1, :seed_count)
    CROSS JOIN day_list AS l
)
SELECT p.customer_id,
       p.account_id,
       p.first_name,
       p.last_name,
       -- born "age" years before the opening day, minus 0-364 days
       (p.open_date - make_interval(years => p.age))::date - floor(random() * 365)::int AS date_of_birth,
       lower(p.first_name || '.' || p.last_name || p.customer_id || '@example.com')      AS email,
       '+6140' || lpad(p.customer_id::text, 7, '0')                                      AS mobile,
       p.marketing_consent,
       p.statement_delivery,
       (1 + floor(random() * 200))::int || ' ' || p.street AS line1,
       split_part(p.home, '|', 1) AS suburb,
       split_part(p.home, '|', 2) AS state,
       split_part(p.home, '|', 3) AS postcode,
       split_part(p.home, '|', 4) AS bsb,
       p.channel,
       -- STUDENT is only for customers under 25; half of them take it,
       -- which is about 10% of all accounts
       CASE WHEN p.age < 25 AND random() < 0.5 THEN 'STUDENT' ELSE p.product END AS product_code,
       -- opening time: a random moment between 07:00 and 22:00 Sydney time
       (p.open_date + time '07:00' + random() * interval '15 hours')
           AT TIME ZONE 'Australia/Sydney' AS opened_at,
       -- the mobile-app session started 3 to 15 minutes before that
       md5('session-' || p.customer_id)::uuid AS session_id,
       (3 + random() * 12)::numeric(4,1)      AS session_minutes,
       p.device,
       p.kyc_result,
       p.kyc_result = 'REFER' AND p.kyc_retry = 'yes' AS kyc_second_attempt,
       p.kyc_retry_result,
       p.watchlist
FROM picked p;

-- Put the whole batch into ONE psql variable (as JSON), so it can be
-- carried to the other database after \connect.
SELECT json_agg(c ORDER BY c.customer_id) AS batch FROM new_customer c \gset


-- #############################################################################
-- 2. identity_fraud: the app's identity checks, written BEFORE the account
--    is opened, so the fraud consumer finds them when AccountOpened arrives
-- #############################################################################
\connect identity_fraud

CREATE TEMP TABLE batch AS
SELECT * FROM json_to_recordset(:'batch') AS b (
    customer_id bigint, session_id uuid, opened_at timestamptz, session_minutes numeric,
    device text, kyc_result text, kyc_second_attempt boolean, kyc_retry_result text,
    watchlist text);

-- A new device for each customer on their own phone (the 4 ring devices
-- already exist, from sql/07_reference_data.sql).
INSERT INTO device (device_fingerprint)
SELECT md5('device-' || customer_id)
FROM batch
WHERE device = 'own';

-- The sign-up session, on the customer's own device or a ring device.
INSERT INTO onboarding_session (session_id, customer_id, device_id, started_at)
SELECT b.session_id,
       b.customer_id,
       d.device_id,
       b.opened_at - b.session_minutes * interval '1 minute'
FROM batch b
JOIN device d
  ON d.device_fingerprint = CASE WHEN b.device = 'own' THEN md5('device-' || b.customer_id)
                                 ELSE md5(b.device)
                            END;

-- KYC: first attempt, and a second attempt for some REFER customers.
INSERT INTO kyc_verification (customer_id, attempt_no, result)
SELECT customer_id, 1, kyc_result FROM batch;

INSERT INTO kyc_verification (customer_id, attempt_no, result)
SELECT customer_id, 2, kyc_retry_result FROM batch WHERE kyc_second_attempt;

-- Watchlist screening.
INSERT INTO watchlist_screening (customer_id, match_status)
SELECT customer_id, watchlist FROM batch;


-- #############################################################################
-- 3. core_banking: open the accounts. ONE transaction: at COMMIT the triggers
--    (sql/03) write CustomerCreated + AccountOpened for every new customer
--    to the outbox, and Debezium sends them to Kafka.
-- #############################################################################
\connect core_banking

BEGIN;

CREATE TEMP TABLE batch AS
SELECT * FROM json_to_recordset(:'batch') AS b (
    customer_id bigint, account_id bigint, first_name text, last_name text,
    date_of_birth date, email text, mobile text, marketing_consent boolean,
    statement_delivery text, line1 text, suburb text, state text, postcode text,
    bsb text, channel text, product_code text, opened_at timestamptz, session_id uuid);

INSERT INTO customer (customer_id, first_name, last_name, date_of_birth, email, mobile,
                      marketing_consent, statement_delivery, customer_since,
                      created_at, updated_at)
SELECT customer_id, first_name, last_name, date_of_birth, email, mobile,
       marketing_consent, statement_delivery, opened_at::date,
       opened_at, opened_at
FROM batch;

INSERT INTO customer_address (customer_id, address_type, line1, suburb, state, postcode, valid_from)
SELECT customer_id, 'RESIDENTIAL', line1, suburb, state, postcode, opened_at::date
FROM batch;

-- New accounts start PENDING (the column default) until the fraud decision
-- is applied by demo/apply_decisions.sql.
INSERT INTO account (account_id, branch_id, account_number, product_code, channel,
                     opened_at, onboarding_session_id, updated_at)
SELECT b.account_id,
       br.branch_id,
       (10000000 + b.account_id)::text,                                  -- 8-digit account number
       b.product_code,
       b.channel,
       b.opened_at,
       b.session_id,                                                     -- same id as in identity_fraud
       b.opened_at
FROM batch b
JOIN branch br ON br.bsb = b.bsb;

INSERT INTO account_holder (account_id, customer_id, holder_role, added_at)
SELECT account_id, customer_id, 'PRIMARY', opened_at
FROM batch;

COMMIT;

-- What was created.
SELECT count(*) AS new_accounts,
       min(account_id) AS first_account_id,
       max(account_id) AS last_account_id,
       min(opened_at)::date AS first_day,
       max(opened_at)::date AS last_day
FROM batch;

\echo 'Done. Watch the consumers:  docker compose logs -f consumer-fraud consumer-crm consumer-loyalty'
\echo 'Once every new account is scored, run demo/apply_decisions.sql to apply the decisions.'
