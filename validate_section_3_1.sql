-- Section 3.1 source-system validation for West City Bank.

\set ON_ERROR_STOP on
\pset pager off
\echo '=== Section 3.1: core_banking ==='
\connect core_banking

SELECT 'core_banking' AS source_database, 7 AS expected_tables,
       count(*) AS actual_tables,
       string_agg(c.relname, ', ' ORDER BY c.relname) AS table_names,
       CASE WHEN count(*) = 7 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p');

SELECT 'core_banking' AS source_database, 5 AS expected_local_fks,
       count(*) AS actual_local_fks,
       CASE WHEN count(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'public' AND c.contype = 'f';

-- A partial unique index prevents a SECOND PRIMARY, but does not require one.
SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname = 'public' AND indexname = 'ux_account_holder_primary';

WITH holder_counts AS (
    SELECT a.account_id,
           count(h.customer_id) FILTER (WHERE h.holder_role = 'PRIMARY') AS primary_count
    FROM account a LEFT JOIN account_holder h ON h.account_id = a.account_id
    GROUP BY a.account_id
)
SELECT count(*) AS total_accounts,
       count(*) FILTER (WHERE primary_count <> 1) AS invalid_accounts,
       CASE WHEN count(*) = 0 THEN 'NO DATA'
            WHEN count(*) FILTER (WHERE primary_count <> 1) = 0 THEN 'PASS'
            ELSE 'FAIL' END AS result
FROM holder_counts;

-- If the preceding result is FAIL, this lists the accounts to investigate.
SELECT a.account_id,
       count(h.customer_id) FILTER (WHERE h.holder_role = 'PRIMARY') AS primary_count
FROM account a LEFT JOIN account_holder h ON h.account_id = a.account_id
GROUP BY a.account_id
HAVING count(h.customer_id) FILTER (WHERE h.holder_role = 'PRIMARY') <> 1
ORDER BY a.account_id;

-- An informational gap: the schema does not require activated_at for ACTIVE.
SELECT count(*) FILTER (WHERE account_status = 'ACTIVE' AND activated_at IS NULL)
           AS active_without_activation_timestamp
FROM account;

-- Expected FK rejection and partial unique-index rejection; no lasting rows.
BEGIN;
DO $validation$
DECLARE
    absent_account bigint;
    absent_customer bigint;
    existing_account bigint;
    temporary_customer bigint;
    rejected boolean;
BEGIN
    SELECT coalesce(min(account_id) - 1, -1) INTO absent_account FROM account;
    SELECT coalesce(min(customer_id) - 1, -1) INTO absent_customer FROM customer;
    rejected := false;
    BEGIN
        INSERT INTO account_holder(account_id, customer_id, holder_role)
        VALUES (absent_account, absent_customer, 'PRIMARY');
    EXCEPTION WHEN foreign_key_violation THEN rejected := true;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'FAIL: missing Core parent accepted'; END IF;
    RAISE NOTICE 'PASS: missing local parent rejected by FK';

    SELECT account_id INTO existing_account FROM account_holder
    WHERE holder_role = 'PRIMARY' LIMIT 1;
    IF existing_account IS NULL THEN
        RAISE NOTICE 'SKIP: second-PRIMARY test requires an existing PRIMARY holder';
    ELSE
        INSERT INTO customer(first_name, last_name, date_of_birth, email, mobile)
        VALUES ('Validation', 'Rollback', DATE '1990-01-01',
                'rollback@example.invalid', '+61400000000')
        RETURNING customer_id INTO temporary_customer;
        rejected := false;
        BEGIN
            INSERT INTO account_holder(account_id, customer_id, holder_role)
            VALUES (existing_account, temporary_customer, 'PRIMARY');
        EXCEPTION WHEN unique_violation THEN rejected := true;
        END;
        IF NOT rejected THEN RAISE EXCEPTION 'FAIL: second PRIMARY accepted'; END IF;
        RAISE NOTICE 'PASS: second PRIMARY rejected by partial unique index';
    END IF;
END
$validation$;
ROLLBACK;

\echo '=== Section 3.1: identity_fraud ==='
\connect identity_fraud
SELECT 'identity_fraud' AS source_database, 6 AS expected_tables,
       count(*) AS actual_tables,
       string_agg(c.relname, ', ' ORDER BY c.relname) AS table_names,
       CASE WHEN count(*) = 6 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p');
SELECT 'identity_fraud' AS source_database, 1 AS expected_local_fks,
       count(*) AS actual_local_fks,
       CASE WHEN count(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'public' AND c.contype = 'f';
SELECT count(*) AS risk_assessments,
       count(*) FILTER (WHERE risk_score NOT BETWEEN 0 AND 100) AS invalid_scores
FROM risk_assessment;

BEGIN;
DO $validation$
DECLARE
    kyc_customer bigint;
    kyc_attempt integer;
    rejected boolean;
BEGIN
    SELECT customer_id, attempt_no INTO kyc_customer, kyc_attempt
    FROM kyc_verification LIMIT 1;
    IF kyc_customer IS NULL THEN
        RAISE NOTICE 'SKIP: duplicate-KYC test requires a KYC row';
    ELSE
        rejected := false;
        BEGIN
            INSERT INTO kyc_verification(customer_id, attempt_no, result)
            VALUES (kyc_customer, kyc_attempt, 'PASS');
        EXCEPTION WHEN unique_violation THEN rejected := true;
        END;
        IF NOT rejected THEN RAISE EXCEPTION 'FAIL: duplicate KYC attempt accepted'; END IF;
        RAISE NOTICE 'PASS: duplicate KYC attempt rejected';
    END IF;
    rejected := false;
    BEGIN
        INSERT INTO risk_assessment(account_id, customer_id, risk_score,
                                    risk_band, decision)
        VALUES (-9223372036854775807, -9223372036854775807, 101,
                'HIGH', 'DECLINE');
    EXCEPTION WHEN check_violation THEN rejected := true;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'FAIL: risk score 101 accepted'; END IF;
    RAISE NOTICE 'PASS: out-of-range risk score rejected';
END
$validation$;
ROLLBACK;

\echo '=== Section 3.1: crm ==='
\connect crm
SELECT 'crm' AS source_database, 4 AS expected_tables,
       count(*) AS actual_tables,
       string_agg(c.relname, ', ' ORDER BY c.relname) AS table_names,
       CASE WHEN count(*) = 4 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p');
SELECT 'crm' AS source_database, 2 AS expected_local_fks,
       count(*) AS actual_local_fks,
       CASE WHEN count(*) = 2 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'public' AND c.contype = 'f';
SELECT count(*) AS contacts, (SELECT count(*) FROM crm_financial_account) AS accounts,
       (SELECT count(*) FROM crm_case) AS cases
FROM crm_contact;

BEGIN;
DO $validation$
DECLARE
    existing_account bigint;
    rejected boolean;
BEGIN
    SELECT account_id INTO existing_account FROM crm_case
    WHERE case_type = 'WELCOME_CALL' LIMIT 1;
    IF existing_account IS NULL THEN
        RAISE NOTICE 'SKIP: duplicate-welcome-call test requires a case';
    ELSE
        rejected := false;
        BEGIN
            INSERT INTO crm_case(account_id, case_type)
            VALUES (existing_account, 'WELCOME_CALL');
        EXCEPTION WHEN unique_violation THEN rejected := true;
        END;
        IF NOT rejected THEN RAISE EXCEPTION 'FAIL: duplicate welcome call accepted'; END IF;
        RAISE NOTICE 'PASS: duplicate welcome call rejected';
    END IF;
END
$validation$;
ROLLBACK;

\echo '=== Section 3.1: loyalty ==='
\connect loyalty
SELECT 'loyalty' AS source_database, 4 AS expected_tables,
       count(*) AS actual_tables,
       string_agg(c.relname, ', ' ORDER BY c.relname) AS table_names,
       CASE WHEN count(*) = 4 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p');
SELECT 'loyalty' AS source_database, 2 AS expected_local_fks,
       count(*) AS actual_local_fks,
       CASE WHEN count(*) = 2 THEN 'PASS' ELSE 'FAIL' END AS result
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'public' AND c.contype = 'f';
SELECT count(*) AS members, (SELECT count(*) FROM member_offer) AS awards
FROM loyalty_member;
-- An ordinary UNIQUE(product_code) allows multiple NULL fallback offers.
SELECT count(*) AS fallback_offers FROM loyalty_offer WHERE product_code IS NULL;

BEGIN;
DO $validation$
DECLARE
    existing_account bigint;
    existing_member bigint;
    existing_offer integer;
    rejected boolean;
BEGIN
    SELECT account_id, member_id, offer_id
    INTO existing_account, existing_member, existing_offer
    FROM member_offer LIMIT 1;
    IF existing_account IS NULL THEN
        RAISE NOTICE 'SKIP: duplicate-award test requires an existing award';
    ELSE
        rejected := false;
        BEGIN
            INSERT INTO member_offer(account_id, member_id, offer_id, source_version)
            VALUES (existing_account, existing_member, existing_offer, 1);
        EXCEPTION WHEN unique_violation THEN rejected := true;
        END;
        IF NOT rejected THEN RAISE EXCEPTION 'FAIL: duplicate award accepted'; END IF;
        RAISE NOTICE 'PASS: duplicate account award rejected';
    END IF;
END
$validation$;
ROLLBACK;
\echo '=== Section 3.1 checks complete. ==='
