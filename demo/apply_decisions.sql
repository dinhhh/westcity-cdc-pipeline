-- =============================================================================
-- File:     demo/apply_decisions.sql
-- Purpose:  Demo step 2b (manual): apply the fraud decision to the account in
--           core_banking (docs/demo-flow.md 2.2).
--             APPROVE -> ACTIVE, activated_at = decision time
--             DECLINE -> REJECTED
--             REVIEW  -> stays PENDING (final in the prototype)
--           The status change fires the core_banking triggers (sql/03), which
--           write AccountActivated / AccountRejected to the outbox; Debezium
--           and Kafka carry it on to CRM and Loyalty.
-- Run:      AFTER the fraud consumer has scored the account
--           (docker compose logs consumer-fraud), in the core_banking database:
--             docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql
--           or paste it into CloudBeaver with core_banking selected.
--           Safe to run again: only PENDING accounts with a decision change.
--
-- Used for the 90-day history (after seed/seed_history.sql) and for the live
-- demo accounts alike.
-- identity_fraud.risk_assessment is a read-only foreign table (postgres_fdw),
-- created in core_banking by sql/07_reference_data.sql.
-- =============================================================================

UPDATE account a
   SET account_status = CASE r.decision WHEN 'APPROVE' THEN 'ACTIVE' ELSE 'REJECTED' END,
       activated_at   = CASE r.decision WHEN 'APPROVE' THEN r.assessed_at END
  FROM identity_fraud.risk_assessment r
 WHERE r.account_id     = a.account_id
   AND r.decision      IN ('APPROVE', 'DECLINE')
   AND a.account_status = 'PENDING'
RETURNING a.account_id, r.decision, a.account_status, a.activated_at, a.version;
