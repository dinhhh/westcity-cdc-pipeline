-- Dashboard views (gold layer) for D1-D3. The app just runs SELECT * on each.
-- Dates are Sydney time; percentages are 0-100.

\connect westcity_dw


-- Freshness: last gold load and account count
CREATE VIEW gold.vw_last_load AS
SELECT (SELECT max(finished_at) FROM ops.etl_run
        WHERE job_name = '03_load_gold' AND status = 'SUCCEEDED') AS last_warehouse_load,
       (SELECT count(*) FROM gold.fact_account_opening)          AS accounts_in_warehouse;


-- D1 Account Opening Growth

-- KPIs: today's openings, approvals, declines; month total
CREATE VIEW gold.vw_d1_kpi_today AS
SELECT count(*) FILTER (WHERE d.calendar_date = current_date)                  AS accounts_opened_today,
       count(*) FILTER (WHERE d.calendar_date = current_date AND f.is_approved) AS approved_today,
       count(*) FILTER (WHERE d.calendar_date = current_date AND f.is_declined) AS declined_today,
       count(*) FILTER (WHERE date_trunc('month', d.calendar_date)
                              = date_trunc('month', current_date))             AS openings_this_month
FROM gold.fact_account_opening f
JOIN gold.dim_date d ON d.date_key = f.open_date_key;

-- Daily openings, last 90 days (zero-filled)
CREATE VIEW gold.vw_d1_daily_openings AS
SELECT d.calendar_date                                    AS open_date,
       count(f.account_id)                                AS openings,
       count(f.account_id) FILTER (WHERE f.is_approved)   AS approved
FROM gold.dim_date d
LEFT JOIN gold.fact_account_opening f ON f.open_date_key = d.date_key
WHERE d.calendar_date BETWEEN current_date - 90 AND current_date
GROUP BY d.calendar_date
ORDER BY d.calendar_date;

-- Openings by product and channel
CREATE VIEW gold.vw_d1_openings_by_product_channel AS
SELECT p.product_code AS product,
       c.channel,
       count(*)       AS openings
FROM gold.fact_account_opening f
JOIN gold.dim_product p ON p.product_key = f.product_key
JOIN gold.dim_channel c ON c.channel_key = f.channel_key
GROUP BY p.product_code, c.channel
ORDER BY p.product_code, c.channel;

-- Openings by segment, with share
CREATE VIEW gold.vw_d1_openings_by_segment AS
SELECT coalesce(f.segment, 'NOT IN CRM YET')                         AS segment,
       count(*)                                                      AS openings,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)            AS pct
FROM gold.fact_account_opening f
GROUP BY coalesce(f.segment, 'NOT IN CRM YET')
ORDER BY openings DESC;

-- Openings by state
CREATE VIEW gold.vw_d1_openings_by_state AS
SELECT cu.residential_state AS state,
       count(*)             AS openings
FROM gold.fact_account_opening f
JOIN gold.dim_customer cu ON cu.customer_key = f.customer_key
GROUP BY cu.residential_state
ORDER BY openings DESC;

-- 15 latest openings
CREATE VIEW gold.vw_d1_latest_openings AS
SELECT to_char(f.opened_at, 'DD Mon HH24:MI:SS') AS opened_at,
       cu.customer_name,
       p.product_code AS product,
       c.channel,
       f.segment,
       f.risk_decision,
       f.loyalty_enrolled
FROM gold.fact_account_opening f
JOIN gold.dim_customer cu ON cu.customer_key = f.customer_key
JOIN gold.dim_product p   ON p.product_key = f.product_key
JOIN gold.dim_channel c   ON c.channel_key = f.channel_key
ORDER BY f.opened_at DESC
LIMIT 15;


-- D2 Onboarding Risk & Compliance

-- Fraud decisions by channel
CREATE VIEW gold.vw_d2_decision_mix AS
SELECT coalesce(f.risk_decision, 'NOT SCORED') AS risk_decision,
       c.channel,
       count(*)                                AS accounts
FROM gold.fact_account_opening f
JOIN gold.dim_channel c ON c.channel_key = f.channel_key
GROUP BY coalesce(f.risk_decision, 'NOT SCORED'), c.channel
ORDER BY risk_decision, c.channel;

-- Risk bands per week
CREATE VIEW gold.vw_d2_risk_band_weekly AS
SELECT d.week_start AS week,
       f.risk_band,
       count(*)     AS accounts
FROM gold.fact_account_opening f
JOIN gold.dim_date d ON d.date_key = f.open_date_key
WHERE f.risk_band IS NOT NULL
GROUP BY d.week_start, f.risk_band
ORDER BY week, f.risk_band;

-- Devices shared by 3+ customers (possible fraud rings)
CREATE VIEW gold.vw_d2_shared_device_rings AS
SELECT left(f.device_fingerprint, 8)                  AS device_id,
       count(*)                                       AS customers_on_device,
       count(*) FILTER (WHERE f.is_declined)          AS declined,
       count(*) FILTER (WHERE f.is_approved)          AS approved
FROM gold.fact_account_opening f
WHERE f.device_fingerprint IS NOT NULL
GROUP BY f.device_fingerprint
HAVING count(*) >= 3
ORDER BY customers_on_device DESC, device_id;

-- Decision speed per week
CREATE VIEW gold.vw_d2_time_to_decision AS
SELECT d.week_start                                                   AS week,
       round(avg(f.minutes_to_decision), 2)                           AS avg_minutes_to_decision,
       round(100.0 * avg((f.minutes_to_decision <= 5)::integer), 1)   AS pct_within_5_min
FROM gold.fact_account_opening f
JOIN gold.dim_date d ON d.date_key = f.open_date_key
WHERE f.minutes_to_decision IS NOT NULL
GROUP BY d.week_start
ORDER BY week;

-- Top risk reasons (comma list split into rows)
CREATE VIEW gold.vw_d2_top_reason_codes AS
SELECT rc.reason_code,
       count(*) AS occurrences
FROM gold.fact_account_opening f
CROSS JOIN LATERAL unnest(string_to_array(f.reason_codes, ',')) AS rc (reason_code)
GROUP BY rc.reason_code
ORDER BY occurrences DESC, rc.reason_code;


-- D3 Customer Engagement & Consistency

-- Loyalty enrolment by segment (~70% expected)
CREATE VIEW gold.vw_d3_loyalty_enrolment_by_segment AS
SELECT coalesce(f.segment, 'NOT IN CRM YET')                                  AS segment,
       count(*) FILTER (WHERE f.is_approved)                                  AS approved_accounts,
       count(*) FILTER (WHERE f.is_approved AND f.loyalty_enrolled)           AS enrolled,
       round(100.0 * count(*) FILTER (WHERE f.is_approved AND f.loyalty_enrolled)
                   / nullif(count(*) FILTER (WHERE f.is_approved), 0), 1)     AS enrolment_pct
FROM gold.fact_account_opening f
GROUP BY coalesce(f.segment, 'NOT IN CRM YET')
ORDER BY segment;

-- Welcome offers per month
CREATE VIEW gold.vw_d3_welcome_offers_monthly AS
SELECT date_trunc('month', f.activated_at)::date                       AS month,
       count(*)                                                        AS offers_issued,
       sum(f.welcome_offer_points)                                     AS points_committed,
       round(100.0 * avg(f.offer_redeemed::integer), 1)                AS redeemed_pct
FROM gold.fact_account_opening f
WHERE f.loyalty_enrolled
GROUP BY date_trunc('month', f.activated_at)::date
ORDER BY month;

-- Welcome-call coverage per week
CREATE VIEW gold.vw_d3_welcome_call_coverage AS
SELECT d.week_start                                                              AS week,
       count(*) FILTER (WHERE f.is_approved)                                     AS approved_accounts,
       count(*) FILTER (WHERE f.welcome_task_created)                            AS welcome_tasks,
       round(100.0 * count(*) FILTER (WHERE f.welcome_task_status = 'COMPLETED')
                   / nullif(count(*) FILTER (WHERE f.welcome_task_created), 0), 1) AS completed_pct
FROM gold.fact_account_opening f
JOIN gold.dim_date d ON d.date_key = f.open_date_key
GROUP BY d.week_start
ORDER BY week;

-- Cross-system consistency checks (all should be 0)
CREATE VIEW gold.vw_d3_consistency_checks AS
SELECT 'Declined but enrolled in loyalty' AS check_name,
       count(*) FILTER (WHERE f.is_declined AND f.loyalty_enrolled) AS violations
FROM gold.fact_account_opening f
UNION ALL
SELECT 'Declined but ACTIVE in CRM',
       count(*) FILTER (WHERE f.is_declined AND f.crm_account_status = 'ACTIVE')
FROM gold.fact_account_opening f
UNION ALL
SELECT 'Approved but missing in CRM',
       count(*) FILTER (WHERE f.is_approved AND f.crm_account_status IS NULL)
FROM gold.fact_account_opening f
UNION ALL
SELECT 'Approved + consented but not enrolled',
       count(*) FILTER (WHERE f.is_approved AND cu.marketing_consent AND NOT f.loyalty_enrolled)
FROM gold.fact_account_opening f
JOIN gold.dim_customer cu ON cu.customer_key = f.customer_key;
