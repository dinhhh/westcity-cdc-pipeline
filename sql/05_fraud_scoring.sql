-- =============================================================================
-- The device, KYC and watchlist rows must already exist (the onboarding app
-- writes them before the account is opened: seed/seed_history.sql for the
-- history, the demo scripts for Mia and Alex).
--
-- Rules:
--   risk_score = base (random 0-25)
--              + 30 if the device is shared by 3 or more customers
--              + 20 if the latest KYC result is REFER
--              + 25 if the watchlist result is POSSIBLE
--              capped at 100
--   risk_band  = LOW < 40, MEDIUM 40-70, HIGH > 70
--   decision   = LOW -> APPROVE, MEDIUM -> REVIEW, HIGH -> DECLINE
--   hard rules = KYC FAIL or watchlist CONFIRMED -> DECLINE, whatever the score
--   reasons    = the signals that added points or forced DECLINE:
--                SHARED_DEVICE, KYC_REFER, WATCHLIST_POSSIBLE, KYC_FAIL,
--                WATCHLIST_CONFIRMED
--   assessed_at = opened_at + random 5 to 120 seconds (never later than now)
-- =============================================================================

\connect identity_fraud

CREATE FUNCTION fraud_assess_account(
    p_account_id  bigint,        -- core_banking account_id
    p_customer_id bigint,        -- core_banking customer_id (PRIMARY holder)
    p_session_id  uuid,          -- account.onboarding_session_id (may be NULL)
    p_opened_at   timestamptz,   -- when the account was opened
    p_event_id    uuid DEFAULT NULL)  -- Kafka event id
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_device_customers integer := 0;       -- customers on same device
    v_kyc_result       varchar(5);         -- latest KYC attempt
    v_watchlist        varchar(9);         -- watchlist result
    v_score            integer;
    v_band             varchar(6);
    v_decision         varchar(7);
    v_reasons          text[] := '{}';     -- reason codes collected below
BEGIN
    -- 0. Skip events already processed
    IF p_event_id IS NOT NULL THEN
        INSERT INTO processed_event (event_id) VALUES (p_event_id)
        ON CONFLICT (event_id) DO NOTHING;
        IF NOT FOUND THEN
            RETURN 'skipped: event already processed';
        END IF;
    END IF;

    -- Seed random by account id (repeatable)
    PERFORM setseed(mod(p_account_id, 10000) / 10000.0);

    -- 1. Signals for this customer
    -- Customers sharing this device
    SELECT count(DISTINCT other.customer_id)
      INTO v_device_customers
      FROM onboarding_session s
      JOIN onboarding_session other ON other.device_id = s.device_id
     WHERE s.session_id = p_session_id;

    -- Latest KYC attempt.
    SELECT result INTO v_kyc_result
      FROM kyc_verification
     WHERE customer_id = p_customer_id
     ORDER BY attempt_no DESC
     LIMIT 1;

    -- Watchlist result.
    SELECT match_status INTO v_watchlist
      FROM watchlist_screening
     WHERE customer_id = p_customer_id;

    -- 2. Score (capped at 100)
    v_score := floor(random() * 26);                       -- base 0-25

    IF v_device_customers >= 3 THEN
        v_score   := v_score + 30;
        v_reasons := v_reasons || 'SHARED_DEVICE'::text;
    END IF;
    IF v_kyc_result = 'REFER' THEN
        v_score   := v_score + 20;
        v_reasons := v_reasons || 'KYC_REFER'::text;
    END IF;
    IF v_watchlist = 'POSSIBLE' THEN
        v_score   := v_score + 25;
        v_reasons := v_reasons || 'WATCHLIST_POSSIBLE'::text;
    END IF;

    v_score := LEAST(v_score, 100);

    -- 3. Band and decision
    v_band := CASE WHEN v_score < 40  THEN 'LOW'
                   WHEN v_score <= 70 THEN 'MEDIUM'
                   ELSE                    'HIGH'
              END;
    v_decision := CASE v_band WHEN 'LOW'    THEN 'APPROVE'
                              WHEN 'MEDIUM' THEN 'REVIEW'
                              ELSE               'DECLINE'
                  END;

    -- Hard decline rules
    IF v_kyc_result = 'FAIL' THEN
        v_decision := 'DECLINE';
        v_reasons  := v_reasons || 'KYC_FAIL'::text;
    END IF;
    IF v_watchlist = 'CONFIRMED' THEN
        v_decision := 'DECLINE';
        v_reasons  := v_reasons || 'WATCHLIST_CONFIRMED'::text;
    END IF;

    -- 4. Save assessment
    INSERT INTO risk_assessment (account_id, customer_id, risk_score, risk_band,
                                 decision, reason_codes, assessed_at)
    VALUES (p_account_id, p_customer_id, v_score, v_band, v_decision,
            NULLIF(array_to_string(v_reasons, ','), ''),
            LEAST(p_opened_at + (5 + random() * 115) * interval '1 second', now()))
    ON CONFLICT (account_id) DO NOTHING;

    RETURN 'scored: ' || v_score || ' ' || v_band || ' -> ' || v_decision
           || COALESCE(' (' || NULLIF(array_to_string(v_reasons, ','), '') || ')', '');
END;
$$;

COMMENT ON FUNCTION fraud_assess_account IS
    'Identity & Fraud enrichment for one new account (docs/demo-flow.md 2.1): risk score, band, decision, reason codes';
