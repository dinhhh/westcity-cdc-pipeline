-- =============================================================================
-- File:     sql/07_reference_data.sql
-- Purpose:  Load the fixed reference data every account needs (branches,
--           products, fraud-ring devices, welcome offers), and the read-only
--           link that the manual decision step uses.
-- Runs:     Automatically after 06_loyalty_onboarding.sql on an empty data
--           volume. It switches database with \connect, so it runs in psql.
--
-- No customers or accounts are created here. The 90-day history is created
-- AFTER the whole stack is running, by seed/seed_history.sql, so that it
-- travels through the real pipeline (outbox -> Debezium -> Kafka ->
-- consumers) exactly like the live demo accounts.
-- =============================================================================


-- #############################################################################
-- 1. core_banking: branches, products, link to identity_fraud
-- #############################################################################
\connect core_banking

-- Branches. BSB format: bank code (93 = West City) + state digit
-- (2 NSW/ACT, 3 VIC, 4 QLD, 5 SA/NT, 6 WA, 7 TAS) + 3-digit branch.
INSERT INTO branch (bsb, branch_name, state) VALUES
    ('936000', 'Perth (Head Office)', 'WA'),
    ('932000', 'Sydney CBD',          'NSW'),
    ('932150', 'Parramatta',          'NSW'),
    ('932600', 'Canberra',            'ACT'),
    ('933000', 'Melbourne CBD',       'VIC'),
    ('934000', 'Brisbane CBD',        'QLD'),
    ('935000', 'Adelaide',            'SA'),
    ('935800', 'Darwin',              'NT'),
    ('937000', 'Hobart',              'TAS');

-- Products (docs/demo-flow.md 2.5).
INSERT INTO product (product_code, product_name, product_type, monthly_fee, interest_rate) VALUES
    ('EVERYDAY', 'Everyday Account', 'TRANSACTION',  0.00, 0.00),
    ('SAVINGS',  'Bonus Saver',      'SAVINGS',      0.00, 4.50),
    ('STUDENT',  'Student Account',  'TRANSACTION',  0.00, 0.00),
    ('PREMIUM',  'Premium Account',  'TRANSACTION', 15.00, 0.50);

-- Read-only link to identity_fraud (postgres_fdw), used by the manual
-- decision step demo/apply_decisions.sql: it lets one UPDATE in
-- core_banking read identity_fraud.risk_assessment.
CREATE EXTENSION postgres_fdw;
CREATE SERVER identity_fraud_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'identity_fraud');
CREATE USER MAPPING FOR westcity_admin SERVER identity_fraud_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA identity_fraud;
IMPORT FOREIGN SCHEMA public LIMIT TO (risk_assessment)
    FROM SERVER identity_fraud_server INTO identity_fraud;


-- #############################################################################
-- 2. identity_fraud: the four fraud-ring devices
--    docs/demo-flow.md 2.1: plant 4 fraud rings, one device shared by several
--    customers each. The devices exist from the start; seed/seed_history.sql
--    puts a few customers on them, and Alex's demo script uses ring device 1.
-- #############################################################################
\connect identity_fraud

INSERT INTO device (device_fingerprint)
SELECT md5('ring-device-' || ring)                                       -- hashed, like every fingerprint
FROM generate_series(1, 4) AS ring;


-- #############################################################################
-- 3. loyalty: welcome offers (docs/demo-flow.md 2.4)
--    product_code NULL = any other product (STUDENT, PREMIUM).
-- #############################################################################
\connect loyalty

INSERT INTO loyalty_offer (product_code, bonus_points) VALUES
    ('EVERYDAY', 5000),
    ('SAVINGS',  3000),
    (NULL,       1000);
