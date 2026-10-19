-- =============================================================================
-- Purpose:  Loyalty enrichment: when an account is approved, enrol the
--           customer (if they gave marketing consent) and give a welcome
--           offer; when an account is rejected, make sure the customer is
--           not left enrolled. 
--
-- Rules:
--   eligibility   enrol ONLY if account_status = ACTIVE AND marketing_consent
--   tier          BRONZE for all new members
--   welcome offer EVERYDAY -> 5,000 points; SAVINGS -> 3,000; any other
--                 product -> 1,000 (rows in loyalty_offer)
--   assigned_at   = activated_at (never later than now)
--   redemption    REDEEMED with 60% chance if assigned more than 14 days
--                 ago, otherwise ASSIGNED
--   rejected      never enrolled; if an enrolled account is rejected, the
--                 member becomes UNENROLLED (compensation)
-- =============================================================================

\connect loyalty

CREATE FUNCTION loyalty_onboard_account(
    p_customer_id       bigint,       -- core_banking customer_id (PRIMARY holder)
    p_account_id        bigint,       -- core_banking account_id
    p_product_code      varchar,      -- picks the welcome offer
    p_account_status    varchar,      -- ACTIVE / REJECTED (others are ignored)
    p_marketing_consent boolean,      -- customer's consent, from the event
    p_activated_at      timestamptz,  -- when the account became ACTIVE
    p_account_version   integer,      -- core account.version
    p_event_id          uuid DEFAULT NULL)  -- Kafka event id
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_member_id   bigint;
    v_offer_id    integer;
    v_points      integer;
    v_assigned_at timestamptz;
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

    -- 1. Rejected: unenrol (version check blocks old events)
    IF p_account_status = 'REJECTED' THEN
        UPDATE loyalty_member m
           SET member_status = 'UNENROLLED',
               synced_at     = now()
          FROM member_offer o
         WHERE o.account_id     = p_account_id
           AND o.member_id      = m.member_id
           AND o.source_version < p_account_version;
        IF FOUND THEN
            RETURN 'unenrolled: account rejected';
        END IF;
        RETURN 'not enrolled: account rejected';
    END IF;

    -- 2. Only ACTIVE accounts with consent
    IF p_account_status <> 'ACTIVE' THEN
        RETURN 'not enrolled: account is ' || p_account_status;
    END IF;
    IF NOT p_marketing_consent THEN
        RETURN 'not enrolled: no marketing consent';
    END IF;

    -- 3. Enrol (or re-enrol) at BRONZE
    INSERT INTO loyalty_member (customer_id)
    VALUES (p_customer_id)
    ON CONFLICT (customer_id) DO UPDATE
       SET member_status = 'ENROLLED',
           synced_at     = now()
    RETURNING member_id INTO v_member_id;

    -- 4. Welcome offer (product match, else default)
    SELECT offer_id, bonus_points
      INTO v_offer_id, v_points
      FROM loyalty_offer
     WHERE product_code = p_product_code OR product_code IS NULL
     ORDER BY product_code IS NULL          -- exact product first
     LIMIT 1;

    v_assigned_at := LEAST(COALESCE(p_activated_at, now()), now());

    INSERT INTO member_offer (account_id, member_id, offer_id, assigned_at,
                              redemption_status, source_version)
    VALUES (p_account_id, v_member_id, v_offer_id, v_assigned_at,
            CASE WHEN v_assigned_at < now() - interval '14 days' AND random() < 0.60
                 THEN 'REDEEMED'
                 ELSE 'ASSIGNED'
            END,
            p_account_version)
    ON CONFLICT (account_id) DO NOTHING;   -- one welcome offer per account

    RETURN 'enrolled: BRONZE, welcome offer ' || v_points || ' points';
END;
$$;

COMMENT ON FUNCTION loyalty_onboard_account IS
    'Loyalty enrichment for one account (docs/demo-flow.md 2.4): enrol if ACTIVE + consent, BRONZE tier, welcome offer; unenrol on rejection';
