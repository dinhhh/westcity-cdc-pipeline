-- CRM onboarding: crm_onboard_account() is called by the CRM consumer for
-- each account event. Sets segment and lifecycle stage, and adds a welcome
-- call for ACTIVE accounts. Rules: docs/demo-flow.md 2.3.

\connect crm

CREATE FUNCTION crm_onboard_account(
    p_customer_id      bigint,       -- core_banking customer_id (PRIMARY holder)
    p_date_of_birth    date,         -- for the segment
    p_customer_version integer,      -- core customer.version
    p_account_id       bigint,       -- core_banking account_id
    p_account_status   varchar,      -- PENDING / ACTIVE / REJECTED
    p_opened_at        timestamptz,  -- when the account was opened
    p_activated_at     timestamptz,  -- when it became ACTIVE (NULL otherwise)
    p_account_version  integer,      -- core account.version
    p_event_id         uuid DEFAULT NULL)  -- Kafka event id
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_age        integer;
    v_segment    varchar(18);
    v_stage      varchar(14);
    v_contact_id bigint;
    v_task_at    timestamptz;
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

    -- 1. Segment by age at opening
    v_age := date_part('year', age(p_opened_at::date, p_date_of_birth));
    v_segment := CASE
                     WHEN v_age < 23  THEN 'STUDENT'
                     WHEN v_age <= 35 THEN 'YOUNG_PROFESSIONAL'
                     WHEN v_age <= 55 THEN 'FAMILY'
                     ELSE                  'SENIOR'
                 END;

    -- 2. Lifecycle stage
    v_stage := CASE
                   WHEN p_account_status = 'REJECTED' THEN 'DECLINED'
                   WHEN p_account_status = 'ACTIVE'
                        AND p_opened_at > now() - interval '30 days' THEN 'NEW_ONBOARDING'
                   WHEN p_account_status = 'ACTIVE' THEN 'ESTABLISHED'
                   ELSE 'UNDER_REVIEW'
               END;

    -- 3. Contact (versioned upsert)
    INSERT INTO crm_contact (customer_id, segment, source_version)
    VALUES (p_customer_id, v_segment, p_customer_version)
    ON CONFLICT (customer_id) DO UPDATE
       SET segment        = EXCLUDED.segment,
           source_version = EXCLUDED.source_version,
           synced_at      = now()
     WHERE EXCLUDED.source_version > crm_contact.source_version;

    SELECT contact_id INTO v_contact_id
    FROM crm_contact WHERE customer_id = p_customer_id;

    -- 4. Financial account (versioned upsert)
    INSERT INTO crm_financial_account (account_id, contact_id, account_status,
                                       lifecycle_stage, source_version)
    VALUES (p_account_id, v_contact_id, p_account_status, v_stage, p_account_version)
    ON CONFLICT (account_id) DO UPDATE
       SET account_status  = EXCLUDED.account_status,
           lifecycle_stage = EXCLUDED.lifecycle_stage,
           source_version  = EXCLUDED.source_version,
           synced_at       = now()
     WHERE EXCLUDED.source_version > crm_financial_account.source_version;

    -- Data is older than what CRM has: stop
    IF NOT FOUND THEN
        RETURN 'skipped: CRM already has a newer version of this account';
    END IF;

    -- 5. Welcome call for ACTIVE accounts
    IF p_account_status = 'ACTIVE' THEN
        v_task_at := LEAST(COALESCE(p_activated_at, now()) + random() * interval '24 hours',
                           now());

        INSERT INTO crm_case (account_id, case_type, case_status, opened_at)
        VALUES (p_account_id,
                'WELCOME_CALL',
                CASE WHEN v_task_at < now() - interval '7 days' AND random() < 0.85
                     THEN 'COMPLETED'
                     ELSE 'OPEN'
                END,
                v_task_at)
        ON CONFLICT (account_id, case_type) DO NOTHING;
    END IF;

    RETURN 'applied: ' || v_segment || ', ' || v_stage;
END;
$$;

COMMENT ON FUNCTION crm_onboard_account IS
    'CRM enrichment for one account: contact + segment, account + lifecycle stage, welcome-call task';
