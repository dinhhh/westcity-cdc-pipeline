\set ON_ERROR_STOP on
\connect westcity_dw

BEGIN;

INSERT INTO ops.etl_run (job_name, script_version)
VALUES ('03_load_gold', 'v1')
RETURNING run_id \gset

CREATE TEMP TABLE cur_customer ON COMMIT DROP AS
SELECT DISTINCT ON (s.hk_customer) h.customer_id, s.*
FROM silver.sat_customer_details s
JOIN silver.hub_customer h USING (hk_customer)
ORDER BY s.hk_customer, s.load_dts DESC;

CREATE TEMP TABLE cur_account ON COMMIT DROP AS
SELECT DISTINCT ON (hk_account) * FROM silver.sat_account_state
ORDER BY hk_account, load_dts DESC;

CREATE TEMP TABLE cur_product ON COMMIT DROP AS
SELECT DISTINCT ON (hk_product) * FROM silver.sat_product_details
ORDER BY hk_product, load_dts DESC;

CREATE TEMP TABLE cur_kyc ON COMMIT DROP AS
SELECT DISTINCT ON (hk_customer) * FROM silver.sat_customer_kyc
ORDER BY hk_customer, load_dts DESC;

CREATE TEMP TABLE cur_risk ON COMMIT DROP AS
SELECT DISTINCT ON (hk_account) * FROM silver.sat_account_risk
ORDER BY hk_account, load_dts DESC;

CREATE TEMP TABLE cur_crm ON COMMIT DROP AS
SELECT DISTINCT ON (hk_account) * FROM silver.sat_account_crm
ORDER BY hk_account, load_dts DESC;

CREATE TEMP TABLE cur_loyalty ON COMMIT DROP AS
SELECT DISTINCT ON (hk_account) * FROM silver.sat_account_loyalty
ORDER BY hk_account, load_dts DESC;

-- Device used by each customer, and how many customers share that device
-- (a fraud-ring signal).
CREATE TEMP TABLE cur_device ON COMMIT DROP AS
SELECT l.hk_customer,
       d.device_fingerprint,
       count(*) OVER (PARTITION BY l.hk_device) AS customers_on_device
FROM silver.link_customer_device l
JOIN silver.hub_device d USING (hk_device);

--dim_date: every day from the first account opening up to today.
INSERT INTO gold.dim_date (date_key, calendar_date, day_of_week, week_start,
                           month, quarter, year, is_business_day)
SELECT to_char(d, 'YYYYMMDD')::integer,
       d,
       extract(isodow FROM d),
       date_trunc('week', d)::date,                                      -- Monday
       extract(month FROM d),
       extract(quarter FROM d),
       extract(year FROM d),
       extract(isodow FROM d) < 6                                        -- Monday to Friday
FROM generate_series((SELECT min(opened_at)::date FROM cur_account), current_date,
                     interval '1 day') AS g
CROSS JOIN LATERAL (SELECT g::date AS d) AS day
ON CONFLICT (date_key) DO NOTHING;

--dim_product: new products are added, names kept up to date (SCD type 1).
INSERT INTO gold.dim_product (product_code, product_name, product_type)
SELECT h.product_code, p.product_name, p.product_type
FROM silver.hub_product h
LEFT JOIN cur_product p USING (hk_product)
ON CONFLICT (product_code) DO UPDATE
   SET product_name = EXCLUDED.product_name,
       product_type = EXCLUDED.product_type;

--dim_channel: every channel seen.
INSERT INTO gold.dim_channel (channel)
SELECT DISTINCT channel FROM cur_account WHERE channel IS NOT NULL
ON CONFLICT (channel) DO NOTHING;

--dim_customer, SCD type 2:
UPDATE gold.dim_customer d
   SET valid_to   = now(),
       is_current = false
  FROM cur_customer c
 WHERE d.customer_id = c.customer_id
   AND d.is_current
   AND (d.customer_name, d.residential_state, d.marketing_consent)
       IS DISTINCT FROM
       (c.first_name || ' ' || c.last_name, c.residential_state, c.marketing_consent);

INSERT INTO gold.dim_customer (customer_id, customer_name, residential_state,
                               marketing_consent, valid_from)
SELECT c.customer_id,
       c.first_name || ' ' || c.last_name,
       c.residential_state,
       c.marketing_consent,
       now()
FROM cur_customer c
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d
                  WHERE d.customer_id = c.customer_id AND d.is_current);

TRUNCATE gold.fact_account_opening;

INSERT INTO gold.fact_account_opening (
    account_id, open_date_key, customer_key, product_key, channel_key,
    opened_at, activated_at, account_status, age_band,
    kyc_result, kyc_attempts, risk_score, risk_band, risk_decision, reason_codes,
    minutes_to_decision, device_fingerprint, shared_device_customers,
    segment, lifecycle_stage, crm_account_status, welcome_task_created, welcome_task_status,
    loyalty_enrolled, tier, welcome_offer_points, offer_redeemed,
    is_approved, is_declined, opening_count, run_id)
SELECT
    ha.account_id,
    to_char(a.opened_at, 'YYYYMMDD')::integer,                           -- Sydney date (database time zone)
    dc.customer_key,
    dp.product_key,
    dch.channel_key,
    -- core banking
    a.opened_at,
    a.activated_at,
    a.account_status,
    CASE WHEN age.years < 25 THEN '18-24'
         WHEN age.years < 35 THEN '25-34'
         WHEN age.years < 45 THEN '35-44'
         WHEN age.years < 55 THEN '45-54'
         WHEN age.years < 65 THEN '55-64'
         ELSE                     '65+'
    END,
    -- identity & fraud
    k.kyc_result,
    k.kyc_attempts,
    r.risk_score,
    r.risk_band,
    r.decision,
    r.reason_codes,
    round(extract(epoch FROM r.assessed_at - a.opened_at) / 60, 2),     -- minutes to decision
    dv.device_fingerprint,
    dv.customers_on_device,
    -- crm
    cr.segment,
    cr.lifecycle_stage,
    cr.crm_account_status,
    cr.welcome_task_status IS NOT NULL,
    cr.welcome_task_status,
    -- loyalty: enrolled = this account earned a welcome offer and the
    -- member is still enrolled
    coalesce(l.member_status = 'ENROLLED', false),
    l.tier,
    l.bonus_points,
    coalesce(l.redemption_status = 'REDEEMED', false),
    -- derived
    a.account_status = 'ACTIVE',
    a.account_status = 'REJECTED',
    1,
    :run_id
FROM silver.hub_account ha
JOIN cur_account a                ON a.hk_account = ha.hk_account
-- the PRIMARY holder and their current dimension row
JOIN silver.link_account_holder lh ON lh.hk_account = ha.hk_account AND lh.holder_role = 'PRIMARY'
JOIN cur_customer c               ON c.hk_customer = lh.hk_customer
JOIN gold.dim_customer dc         ON dc.customer_id = c.customer_id AND dc.is_current
-- product and channel dimensions
JOIN silver.link_account_product lp ON lp.hk_account = ha.hk_account
JOIN silver.hub_product hp        ON hp.hk_product = lp.hk_product
JOIN gold.dim_product dp          ON dp.product_code = hp.product_code
JOIN gold.dim_channel dch         ON dch.channel = a.channel
-- what the other systems added
LEFT JOIN cur_kyc k               ON k.hk_customer = c.hk_customer
LEFT JOIN cur_risk r              ON r.hk_account = ha.hk_account
LEFT JOIN cur_device dv           ON dv.hk_customer = c.hk_customer
LEFT JOIN cur_crm cr              ON cr.hk_account = ha.hk_account
LEFT JOIN cur_loyalty l           ON l.hk_account = ha.hk_account
-- age at opening, in whole years
CROSS JOIN LATERAL (SELECT date_part('year', age(a.opened_at::date, c.date_of_birth))::integer AS years) AS age;

UPDATE ops.etl_run
   SET finished_at = now(),
       status      = 'SUCCEEDED',
       rows_read   = (SELECT count(*) FROM silver.hub_account),
       rows_loaded = (SELECT count(*) FROM gold.fact_account_opening)
 WHERE run_id = :run_id;

SELECT count(*)                                  AS fact_rows,
       count(*) FILTER (WHERE is_approved)       AS approved,
       count(*) FILTER (WHERE is_declined)       AS declined,
       count(*) FILTER (WHERE risk_decision IS NULL) AS not_scored_yet
FROM gold.fact_account_opening;

COMMIT;
