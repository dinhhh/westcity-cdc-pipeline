-- core_banking outbox: triggers write an event row in the same transaction
-- as each customer/account change. Debezium reads the outbox into Kafka.
-- Some triggers are DEFERRED so they see rows added later in the transaction.

\connect core_banking


-- 1. Payload builders (full current state)

-- Customer + current residential address
CREATE FUNCTION customer_snapshot(p_customer_id bigint)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
    SELECT jsonb_build_object(
        'customer_id',         c.customer_id,
        'version',             c.version,
        'first_name',          c.first_name,
        'last_name',           c.last_name,
        'date_of_birth',       c.date_of_birth,
        'email',               c.email,
        'mobile',              c.mobile,
        'marketing_consent',   c.marketing_consent,
        'statement_delivery',  c.statement_delivery,
        'customer_since',      c.customer_since,
        'residential_address', (SELECT jsonb_build_object(
                                           'line1',    a.line1,
                                           'suburb',   a.suburb,
                                           'state',    a.state,
                                           'postcode', a.postcode)
                                FROM customer_address a
                                WHERE a.customer_id  = c.customer_id
                                  AND a.address_type = 'RESIDENTIAL'
                                  AND a.valid_to IS NULL)          -- NULL if none yet
    )
    FROM customer c
    WHERE c.customer_id = p_customer_id;
$$;

-- Account + BSB + holders (with a customer copy, topic order isn't guaranteed)
CREATE FUNCTION account_snapshot(p_account_id bigint)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
    SELECT jsonb_build_object(
        'account_id',            ac.account_id,
        'version',               ac.version,
        'account_number',        ac.account_number,
        'bsb',                   b.bsb,
        'branch_id',             ac.branch_id,
        'product_code',          ac.product_code,
        'account_status',        ac.account_status,
        'channel',               ac.channel,
        'opened_at',             ac.opened_at,
        'activated_at',          ac.activated_at,
        'closed_at',             ac.closed_at,
        'onboarding_session_id', ac.onboarding_session_id,
        'holders', COALESCE((SELECT jsonb_agg(
                                        jsonb_build_object(
                                            'customer_id', h.customer_id,
                                            'holder_role', h.holder_role,
                                            'customer',    customer_snapshot(h.customer_id))
                                        ORDER BY h.holder_role DESC, h.customer_id)  -- PRIMARY first
                             FROM account_holder h
                             WHERE h.account_id = ac.account_id),
                            '[]'::jsonb)
    )
    FROM account ac
    JOIN branch b ON b.branch_id = ac.branch_id
    WHERE ac.account_id = p_account_id;
$$;


-- 2. Outbox writer (adds the event envelope)
CREATE FUNCTION write_outbox(p_aggregatetype text,     -- picks Kafka topic
                             p_aggregateid   bigint,   -- Kafka key
                             p_type          text,     -- event type, e.g. 'AccountOpened'
                             p_version       integer,  -- aggregate version after the change
                             p_data          jsonb)    -- customer or account snapshot
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_id uuid := gen_random_uuid();
BEGIN
    INSERT INTO outbox (id, aggregatetype, aggregateid, type, event_version, payload)
    VALUES (v_event_id,
            p_aggregatetype,
            p_aggregateid::text,
            p_type,
            p_version,
            jsonb_build_object(
                'schema_version',    1,                -- contract version
                'event_id',          v_event_id,
                'event_type',        p_type,
                'aggregate_type',    p_aggregatetype,
                'aggregate_id',      p_aggregateid::text,
                'aggregate_version', p_version,
                'occurred_at',       now(),            -- transaction timestamp
                'producer',          'core_banking',
                'data',              p_data));
END;
$$;


-- 3. Version triggers: +1 when a business column changes

CREATE FUNCTION trg_customer_bump_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER customer_bump_version
    BEFORE UPDATE ON customer
    FOR EACH ROW
    WHEN ((OLD.first_name, OLD.last_name, OLD.date_of_birth, OLD.email, OLD.mobile,
           OLD.marketing_consent, OLD.statement_delivery, OLD.customer_since)
          IS DISTINCT FROM
          (NEW.first_name, NEW.last_name, NEW.date_of_birth, NEW.email, NEW.mobile,
           NEW.marketing_consent, NEW.statement_delivery, NEW.customer_since))
    EXECUTE FUNCTION trg_customer_bump_version();

CREATE FUNCTION trg_account_bump_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();   -- time of the last change
    RETURN NEW;
END;
$$;

CREATE TRIGGER account_bump_version
    BEFORE UPDATE ON account
    FOR EACH ROW
    WHEN ((OLD.branch_id, OLD.account_number, OLD.product_code, OLD.account_status,
           OLD.channel, OLD.activated_at, OLD.closed_at, OLD.onboarding_session_id)
          IS DISTINCT FROM
          (NEW.branch_id, NEW.account_number, NEW.product_code, NEW.account_status,
           NEW.channel, NEW.activated_at, NEW.closed_at, NEW.onboarding_session_id))
    EXECUTE FUNCTION trg_account_bump_version();


-- 4. Event triggers

-- 4a. CustomerCreated (deferred, includes address)
CREATE FUNCTION trg_customer_created()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_version integer;
BEGIN
    -- Version at commit time
    SELECT version INTO v_version FROM customer WHERE customer_id = NEW.customer_id;
    PERFORM write_outbox('customer', NEW.customer_id, 'CustomerCreated',
                         v_version, customer_snapshot(NEW.customer_id));
    RETURN NULL;   -- ignored for AFTER triggers
END;
$$;

CREATE CONSTRAINT TRIGGER customer_created_event
    AFTER INSERT ON customer
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_customer_created();

-- 4b. CustomerDetailsUpdated
CREATE FUNCTION trg_customer_updated()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM write_outbox('customer', NEW.customer_id, 'CustomerDetailsUpdated',
                         NEW.version, customer_snapshot(NEW.customer_id));
    RETURN NULL;
END;
$$;

CREATE TRIGGER customer_updated_event
    AFTER UPDATE ON customer
    FOR EACH ROW
    WHEN ((OLD.first_name, OLD.last_name, OLD.date_of_birth, OLD.email, OLD.mobile,
           OLD.marketing_consent, OLD.statement_delivery, OLD.customer_since)
          IS DISTINCT FROM
          (NEW.first_name, NEW.last_name, NEW.date_of_birth, NEW.email, NEW.mobile,
           NEW.marketing_consent, NEW.statement_delivery, NEW.customer_since))
    EXECUTE FUNCTION trg_customer_updated();

-- 4c. AccountOpened (deferred, includes holders)
CREATE FUNCTION trg_account_opened()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_version integer;
BEGIN
    SELECT version INTO v_version FROM account WHERE account_id = NEW.account_id;
    PERFORM write_outbox('account', NEW.account_id, 'AccountOpened',
                         v_version, account_snapshot(NEW.account_id));
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER account_opened_event
    AFTER INSERT ON account
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trg_account_opened();

-- 4d. AccountActivated / AccountRejected
CREATE FUNCTION trg_account_status_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM write_outbox('account', NEW.account_id,
                         CASE NEW.account_status
                             WHEN 'ACTIVE'   THEN 'AccountActivated'
                             WHEN 'REJECTED' THEN 'AccountRejected'
                         END,
                         NEW.version, account_snapshot(NEW.account_id));
    RETURN NULL;
END;
$$;

CREATE TRIGGER account_status_event
    AFTER UPDATE OF account_status ON account
    FOR EACH ROW
    WHEN (OLD.account_status IS DISTINCT FROM NEW.account_status
          AND NEW.account_status IN ('ACTIVE','REJECTED'))
    EXECUTE FUNCTION trg_account_status_changed();


-- 5. Publication for Debezium (outbox only)
CREATE PUBLICATION dbz_outbox_publication FOR TABLE outbox;
