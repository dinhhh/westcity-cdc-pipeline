-- Reference data: branches, products, fraud-ring devices, welcome offers.
-- Customers and accounts are added later by seed/seed_history.sql.


-- 1. core_banking
\connect core_banking

-- Branches (BSB = 93 + state digit + branch number)
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

-- Products
INSERT INTO product (product_code, product_name, product_type, monthly_fee, interest_rate) VALUES
    ('EVERYDAY', 'Everyday Account', 'TRANSACTION',  0.00, 0.00),
    ('SAVINGS',  'Bonus Saver',      'SAVINGS',      0.00, 4.50),
    ('STUDENT',  'Student Account',  'TRANSACTION',  0.00, 0.00),
    ('PREMIUM',  'Premium Account',  'TRANSACTION', 15.00, 0.50);

-- FDW link to identity_fraud (used by demo/apply_decisions.sql)
CREATE EXTENSION postgres_fdw;
CREATE SERVER identity_fraud_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'identity_fraud');
CREATE USER MAPPING FOR westcity_admin SERVER identity_fraud_server
    OPTIONS (user 'westcity_admin', password 'lab_admin');
CREATE SCHEMA identity_fraud;
IMPORT FOREIGN SCHEMA public LIMIT TO (risk_assessment)
    FROM SERVER identity_fraud_server INTO identity_fraud;


-- 2. identity_fraud: four fraud-ring devices
\connect identity_fraud

INSERT INTO device (device_fingerprint)
SELECT md5('ring-device-' || ring)                                       -- hashed
FROM generate_series(1, 4) AS ring;


-- 3. loyalty: welcome offers (NULL product = any other)
\connect loyalty

INSERT INTO loyalty_offer (product_code, bonus_points) VALUES
    ('EVERYDAY', 5000),
    ('SAVINGS',  3000),
    (NULL,       1000);
