# West City Bank – Event-Driven Integration Prototype (UTS 32113 Assignment 2)

> **docs/demo-flow.md is the spec for: the demo story, enrichment rules, synthetic data, the warehouse fact table and the dashboards.** It extends this file and WINS wherever they conflict on those topics, EXCEPT for the user's overrides listed in "Decisions overriding the spec" below. Read both before working on any of them. Sections below marked *(superseded)* are kept only as background.

## Decisions overriding the spec (user, 30 Sep 2026)
- **No `ip_risk_flag`.** onboarding_session gets no ip_risk_flag column. The risk score has no "+10 IP" term and there is no IP_RISK reason code. Score = base 0-25, +30 shared device (3+ customers), +20 latest KYC REFER, +25 watchlist POSSIBLE, capped at 100.
- **No REVIEW outcome.** A REVIEW decision is final in the prototype, both history and live: the account stays PENDING, CRM lifecycle_stage = UNDER_REVIEW, and there is no final_decision column. minutes_to_decision = risk_assessment.assessed_at - account.opened_at.
- **Decision applied manually.** Step 2b (APPROVE -> ACTIVE, DECLINE -> REJECTED) is a manual SQL step, demo/apply_decisions.sql, for history and live accounts alike: one UPDATE in core_banking that reads identity_fraud.risk_assessment through a postgres_fdw foreign table (schema `identity_fraud` inside core_banking, created by sql/07_reference_data.sql). The fraud consumer does NOT write to core_banking.
- **Demo scripts write the identity data themselves.** They insert device, onboarding_session, kyc_verification and watchlist_screening rows in identity_fraud before opening the account. They span two databases, so they run through psql (\connect), not CloudBeaver.
- **Alex fails KYC** (instead of REFER). He uses a device already shared by 5 customers and has KYC FAIL, so he is declined by the hard rule. His score is only 30 + base (about 30-55), so risk_band may be LOW/MEDIUM next to decision DECLINE. Reason codes: SHARED_DEVICE,KYC_FAIL.
- **Warehouse uses medallion layers:** schema `bronze` (raw data: Kafka-landed core events + SQL extracts), schema `silver` (Data Vault 2.0: hubs, links, insert-only satellites), and schema `gold` (star schema: dims + fact_account_opening + all dashboard views). The spec's `curated.<view>` means `gold.<view>`. The `ops` schema stays separate for audit and watermarks.
- **Consumers:** four compose services (consumer-fraud, consumer-crm, consumer-loyalty, consumer-dw) built from python/Dockerfile (lab python:3.11-slim + pinned psycopg2-binary, confluent-kafka, pandas, plotly), all running consumers/consumer.py <fraud|crm|loyalty|dw>, plus a `python` service (whole project at /workspace) for the dashboards. Scene 6 = `docker compose stop consumer-loyalty`.
- **Unused tables dropped** (user, 30 Sep): ops.recon_sla/recon_run/recon_break/consumer_health, mart.fact_event_propagation and dim_branch are gone.
- **History goes through Kafka** (user, 30 Sep, replacing the earlier "seeded directly" rule). seed/seed_history.sql runs AFTER the stack is up and opens accounts in core_banking with the triggers ON, so history travels outbox -> Debezium -> Kafka -> consumers exactly like Mia and Alex. See "Synthetic history" below.
- **Warehouse gets core events through a 4th Kafka consumer** (user, 30 Sep; overrides "NO DW lander consumer" and demo-flow.md section 4). consumer-dw lands every event of bank.account.events and bank.customer.events, unchanged, in bronze.core_event via bronze.land_core_event() (idempotent by event_id; keeps Kafka topic/partition/offset for lineage). Core customers/accounts are therefore NOT SQL-extracted; only core_banking.product is (full copy, for product names). Fraud, CRM and Loyalty results are NOT on Kafka (only core_banking publishes), so they reach the warehouse by SQL extract (postgres_fdw + watermark). Mind Kafka's default 7-day retention if bronze is ever rebuilt from the topics.

## Project context
- Group assignment for UTS 32113 Advanced Database. Due 11:59pm Friday 16 October 2026.
- Case: West City Bank, a customer opening a new transaction account in the mobile app. The account-opening event must reach the fraud engine, CRM, loyalty and the data warehouse reliably, without blocking the customer.
- Scope is ACCOUNT OPENING only. Money transfers are out of scope.
- A non-working prototype scores zero, so reliability and reproducibility come before features.
- Deliverables for the prototype: at least 3 source databases, at least 1 integrated warehouse, fully commented SQL scripts with synthetic data (create, extract, transform, load) that run without errors, at least 3 reports/dashboards, and a recorded end-to-end demo.
- BUILD_PLAN.md is OUTDATED (it describes the first attempt, deleted on 29 Sep). The revised plan agreed on 30 Sep replaces it.
- docs/event-contract.md was removed from the project. Until it is restored, the event payload format is defined by the functions in sql/03_outbox_triggers.sql.

## Mandatory stack
- PostgreSQL 15 (all source databases and the warehouse; same version as the course lab)
- Apache Kafka (event transport, KRaft mode)
- Debezium (change data capture, via Kafka Connect), for core_banking.outbox only (see Pipeline decisions)
- Everything runs with Docker Compose (separate containers). Not a single Dockerfile.
- RESTARTED FROM SCRATCH on 29 Sep 2026, building step by step. Current state (30 Sep):
  - Compose services: postgres (also mounts ./etl at /etl), cloudbeaver (web SQL editor, localhost:8978), kafka (KRaft, single broker), connect (Debezium 3.6.3), connect-init (a one-shot curl that registers connectors/core-outbox.json), consumer-fraud / consumer-crm / consumer-loyalty / consumer-dw, python.
  - The postgres healthcheck uses `pg_isready -h localhost` (TCP), so it only passes after ALL init scripts have finished (the init server listens on the socket only).
  - SQL (init): 01_create_databases, 02_create_tables, 03_outbox_triggers (triggers + publication dbz_outbox_publication), 04_crm_onboarding, 05_fraud_scoring, 06_loyalty_onboarding (one rule function per system), 07_reference_data (branches, products, 4 ring devices, loyalty offers, core->identity_fraud fdw link), 08_warehouse_setup (fdw links westcity_dw -> the 4 sources, starting watermarks, bronze.land_core_event), 09_reports_dashboards (all gold.vw_d1/d2/d3 views). No customers or accounts at init.
  - Manual scripts: seed/seed_history.sql (history through Kafka), demo/apply_decisions.sql (step 2b), etl/run_all.sql (= 01_extract_bronze, 02_load_silver, 03_load_gold), dashboards/build_dashboards.py.
  - Full run order: `docker compose up -d --build` -> seed -> (wait for consumer-fraud) -> apply_decisions -> `docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/run_all.sql` -> `docker compose exec python python /workspace/dashboards/build_dashboards.py` -> open dashboards/output/*.html.
  - Stage 1 verified end-to-end (30 Sep): seed 10 and 2,000 accounts -> outbox -> Debezium -> Kafka -> fraud score -> apply_decisions -> Activated/Rejected -> CRM + loyalty (2,000 accounts in about 5 s); spec distributions met; all D3 checks 0; replay is skipped (idempotent); a stopped consumer catches up on restart.
  - Stage 2 verified (30 Sep): consumer-dw landed every outbox event exactly once; run_all loads bronze -> silver -> gold; re-run adds 0 silver rows; incremental load after 2,000 more accounts; a customer change gives a 2nd satellite version and an SCD2 row; a live account opened today appears in vw_d1_kpi_today; 3 dashboards built (6/5/4 charts); D3 checks 0.
  - Next: Stage 3 demo scripts (demo/01_open_account_mia.sql, 02_open_account_alex_fraud.sql, 03_trace_account.sql, 04 = etl/run_all.sql), then rehearsal and docs.
- Run: `docker compose up -d --build`. Rebuild the data: `docker compose down && docker volume rm westcity_pgdata westcity_kafkadata && docker compose up -d --build`. Reset Postgres and Kafka together, because Debezium's saved position (and the consumers' offsets) must match the database.
- SQL scripts live FLAT in sql/ (e.g. sql/01_create_databases.sql, sql/02_...). The folder is mounted at /docker-entrypoint-initdb.d.
  - Postgres runs them in name order, ONLY when the data volume is empty, and stops at the first error.
  - Any SQL change requires a rebuild from zero.
  - Subfolders are ignored.
- Keep everything simple: hard-coded values (host, user, password), no .env, Makefile or helper scripts unless the user asks.
- ONE database user for everything: westcity_admin / lab_admin (lab only), host port 5433. No per-service roles or grants.

## Pipeline decisions (30 Sep 2026, from docs/demo-flow.md section 4; supersede earlier decisions)
- **Core Banking -> other systems:** outbox + Debezium + Kafka (unchanged).
- **Kafka -> Identity & Fraud, CRM, Loyalty:** small Python consumers running on the course-lab `python` container image (python:3.11-slim + requirements). They must be idempotent (processed_event) and use versioned upserts. consumer-dw lands core events in the warehouse (see the overrides at the top).
  - consumers/consumer.py: subscribes to bank.account.events only; group `<name>-consumer`; manual offset commit AFTER the DB commit; crashes on any error (Docker restarts it and the event is re-read). fraud handles AccountOpened; crm handles Opened/Activated/Rejected; loyalty handles Activated/Rejected.
  - Debezium's JSON expansion LEAVES OUT null fields (e.g. activated_at of a PENDING account), so optional payload fields must be read with .get().
- **Identity & Fraud, CRM, Loyalty -> warehouse:** SQL extract scripts, NOT Debezium. They are incremental, using a watermark table ops.extract_watermark. The systems are separate databases, so the extracts read them through postgres_fdw. The earlier plan for table-CDC connectors (cdc.* topics) is DROPPED.
- **Core Banking -> warehouse:** SQL extract of the customer/account tables is replaced by the events consumer-dw lands in bronze.core_event (plus a full copy of the product table).
- **All extract/transform/load logic is SQL,** fully commented and runnable top to bottom.
- **Enrichment:** each system adds only its own fields, following docs/demo-flow.md section 2 exactly. Any column without a stated rule gets a plausible random value of the correct type.
- **Synthetic history** (90 days, ending yesterday) comes from seed/seed_history.sql, run manually with psql AFTER `docker compose up`, then demo/apply_decisions.sql once consumer-fraud has scored the accounts. It goes through the real pipeline (user, 30 Sep), NOT directly into each system.
  - The count is one psql variable at the top: `\set seed_count 10` (user: 10 for now; the spec wants about 2,000). Re-running ADDS another batch with new ids (same setseed(0.42), so the same picks).
  - User's method: every field is a random pick from a HARD-CODED LIST (names, 25 ages, 12 suburb|state|postcode|home-BSB entries, products, channels, KYC, watchlist, device), via a temp function pg_temp.pick(choices, weights). Weights give the spec percentages; the opening day is a weighted pick of the last 90 days (trend 100 -> 130, weekends x0.6).
  - It plays the mobile app: it generates the batch in core_banking (ids from the sequences), carries it as JSON in a psql variable (\gset), writes device/session/KYC/watchlist in identity_fraud FIRST, then inserts customer/address/account/holder in core_banking in ONE transaction (triggers -> outbox).
  - Keys: onboarding_session_id = md5('session-' || customer_id)::uuid; own devices md5('device-' || customer_id); fraud-ring devices md5('ring-device-1'..'4') (from sql/07). About 1.3% of customers land on a ring device, so ring sizes are random (6, 4, 8, 7 at 2,000); Alex's script (Stage 3) must not assume exactly 5.
  - Timestamps are backdated (opened_at in the past); the rule functions derive assessed_at, activated_at, task and offer dates from it, so history looks historic even though it flows through Kafka now.
  - Each rule function seeds random() per account (setseed(mod(account_id,10000)/10000.0)), so results do not depend on processing order.
- **Dashboards:**
  - Every report query is a commented SQL view in the gold layer, in sql/09_reports_dashboards.sql.
  - dashboards/build_dashboards.py (Plotly) only runs `SELECT * FROM gold.<view>` and writes one HTML file per dashboard to dashboards/output/. There is no filtering, joining or calculating in Python.
  - It is run with `docker compose exec python python /workspace/dashboards/build_dashboards.py`.
  - Metabase and Grafana are dropped.
- Git is managed manually by the team. ops.etl_run.script_version is a hard-coded version label in the ETL script (e.g. 'v1').
- A1 places West City in Western Australia; we still use Australia/Sydney as head-office business time. State this in the report.

## Architecture
```
core_banking.customer/account --(trigger, same transaction)--> core_banking.outbox
  --> Debezium outbox event router --> Kafka topics bank.customer.events / bank.account.events
      (key = customer_id / account_id)
      --> identity_fraud consumer (KYC/device/watchlist signals -> risk score, band, decision)
      --> crm consumer (contact + account, segment, lifecycle stage, welcome-call task)
      --> loyalty consumer (enrol if ACTIVE + consent, tier, welcome offer)
      --> DW consumer (lands every core event, unchanged, in westcity_dw bronze.core_event)
core_banking.account status <- risk decision, applied MANUALLY by a demo SQL step (APPROVE -> ACTIVE, DECLINE -> REJECTED; emits AccountActivated/Rejected)
history: seed/seed_history.sql (run after startup) opens accounts in core_banking, so it takes the same path as live accounts
westcity_dw bronze <- consumer-dw (core events, Kafka) + SQL extract (postgres_fdw, watermark) from identity_fraud, crm, loyalty (+ core product)
westcity_dw (medallion): bronze (raw) -> silver (Data Vault 2.0) -> gold (star schema + dashboard views); ops (audit, DQ, watermarks); loaded by etl/run_all.sql
dashboards/build_dashboards.py: SELECT * FROM gold views -> Plotly HTML
```
All five databases live in one PostgreSQL container (a stated lab simplification).

## Databases and tables (base columns: westcity-er-model-source.md; extra enrichment columns: docs/demo-flow.md section 2)
### core_banking (system of record)
customer, customer_address, branch (6-digit BSB), product, account (status PENDING/ACTIVE/REJECTED/CLOSED, channel MOBILE/BRANCH/WEB, version, onboarding_session_id, updated_at = set by the version trigger, the warehouse watermark), account_holder (M:N, PRIMARY/JOINT), outbox.
- account.onboarding_session_id is a LOGICAL reference to identity_fraud.onboarding_session (no FK). The fraud consumer uses it to find the device and session.
- Outbox columns use Debezium defaults: id (uuid), aggregatetype, aggregateid, type, payload (jsonb), plus event_version, tx_id and created_at.
  - event_version = the aggregate's version after the change (drives the consumers' versioned upsert).
  - tx_id = txid_current(), for lineage.
- Triggers on customer/account write the outbox row in the SAME transaction and increment `version` when business columns change:
  - customer INSERT -> CustomerCreated; customer UPDATE -> CustomerDetailsUpdated.
  - account INSERT -> AccountOpened, from a DEFERRED constraint trigger that runs at commit, so the account_holder rows already exist.
  - account status -> ACTIVE / REJECTED -> AccountActivated / AccountRejected.
- Debezium publication must include ONLY core_banking.outbox (not the whole database).
- Event types: CustomerCreated, CustomerDetailsUpdated, AccountOpened, AccountActivated, AccountRejected (and AccountSnapshot, allowed by the outbox CHECK but not currently used).

### identity_fraud
device (device_fingerprint), onboarding_session (customer_id, device_id, started_at), kyc_verification (customer_id, attempt_no, result), watchlist_screening (customer_id UK, match_status), risk_assessment (account_id UK, risk_score 0-100, risk_band, decision APPROVE/REVIEW/DECLINE, reason_codes, assessed_at), processed_event. Trimmed to dashboard columns plus keys (30 Sep). Enrichment rules: docs/demo-flow.md 2.1 with the overrides above.
- All scoring rules live in ONE SQL function, fraud_assess_account(...), in sql/05_fraud_scoring.sql. assessed_at = opened_at + 5-120 s, capped at now().

### crm
crm_contact (customer_id UK, segment), crm_financial_account (account_id UK, account_status, lifecycle_stage), crm_case (WELCOME_CALL tasks: account_id, case_status OPEN/COMPLETED, opened_at), processed_event. Tables keep ONLY dashboard columns plus keys and sync columns (user rule, 30 Sep).
- All CRM rules (docs/demo-flow.md 2.3) live in ONE SQL function, crm_onboard_account(...), in sql/04_crm_onboarding.sql. The CRM consumer calls it per account event, passing the event id; the history seed calls it per account, with no event id. It is idempotent (processed_event) and does versioned upserts.
- lifecycle_stage is stored on crm_financial_account, not crm_contact as the spec says, because it follows the account status and must be guarded by the account version.
- The welcome task's opened_at = activated_at + random 0-24 h, capped at now(), so live tasks never start in the future.

### loyalty
loyalty_member (customer_id UK, member_status, tier), loyalty_offer (product_code UK, bonus_points; NULL product = any other product), member_offer (account_id PK, member_id, offer_id, assigned_at, redemption_status, source_version), processed_event. Trimmed (30 Sep): loyalty_account_link and the `eligible` flag were removed; member_offer is the per-account row.
- All loyalty rules live in ONE SQL function, loyalty_onboard_account(...), in sql/06_loyalty_onboarding.sql.
- Enrol only if account_status = ACTIVE AND marketing_consent = true. An AccountRejected for an enrolled account sets member_status = UNENROLLED.

### westcity_dw
- bronze: core_event (Kafka landing: event_id PK, envelope fields, payload jsonb, kafka_topic/partition/offset) + one table per extracted source table, named <system>_<table> (core_product, fraud_device, fraud_onboarding_session, fraud_kyc_verification, fraud_watchlist_screening, fraud_risk_assessment, crm_contact, crm_financial_account, crm_case, loyalty_offer, loyalty_member, loyalty_member_offer), each with run_id + loaded_at.
  - Watermarks (ops.extract_watermark): insert-only tables by growing id (device_id, kyc_id, screening_id, assessment_id, case_id); updatable tables by synced_at (crm_contact, crm_financial_account, loyalty_member, member_offer). Full copy each run (TRUNCATE + INSERT): core product, loyalty_offer, onboarding_session (its started_at is backdated, so unusable as a watermark). Incremental tables are append logs; silver takes the latest copy per key.
- silver (Data Vault 2.0; the ER model calls it raw_vault): hub_customer, hub_account, hub_product, hub_device; link_account_holder, link_account_product, link_customer_device; sat_customer_details, sat_account_state, sat_product_details (core), sat_customer_kyc, sat_account_risk (identity_fraud), sat_account_crm, sat_account_loyalty (added: not in the ER model). Hash keys = md5(business key); hash_diff = md5(ROW(attributes)::text). Satellites are INSERT-ONLY (new row only when hash_diff changes) with load_dts, record_source, run_id; hubs and links also carry run_id. Hubs for customer/account are loaded from core events only; other systems' rows load only for keys already in a hub.
  - Data-quality rules in 02_load_silver move failing rows to ops.dq_quarantine (DELETE ... RETURNING): ACCOUNT_HAS_PRIMARY_HOLDER, CUSTOMER_IS_ADULT.
- gold (the ER model calls it mart): dim_date (+ week_start), dim_customer (SCD type 2: customer_name, residential_state, marketing_consent), dim_product, dim_channel; fact_account_opening = one row per account with the docs/demo-flow.md section 5 columns plus crm_account_status, device_fingerprint and run_id (age_band is on the fact: age at opening). The fact is REBUILT in full each gold load (TRUNCATE + INSERT, one transaction). Views: sql/09 (vw_d2_shared_device_rings.device_id = first 8 chars of the fingerprint).
- ops: etl_run (one row per etl script run), dq_quarantine, extract_watermark. Every etl script is ONE transaction and stamps run_id on every row it writes.

## Conventions (apply everywhere)
- Cross-system references are LOGICAL keys (customer_id, account_id, onboarding_session_id), never foreign keys across databases.
- Consumers are idempotent: skip event_id already in processed_event. The processed_event insert, the business upsert and the DB commit happen in one transaction, and the Kafka offset is committed after it.
- Versioned upsert: `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE excluded.source_version > target.source_version`.
- Delivery is at-least-once; ordering is per key only, and there is NO ordering between the customer and account topics. Every account event therefore embeds a snapshot of each holder's customer (with customer version), so consumers can work from account events alone.
- Every warehouse load opens an ops.etl_run row and stamps run_id on every row it writes; failed rows go to ops.dq_quarantine with a reason.
- SQL scripts must be fully commented and runnable top to bottom without errors.
- Timestamps as timestamptz; business dates in Australia/Sydney.
- Cross-database reads use postgres_fdw (read-only, no cross-database FKs; a lab simplification).

## Use cases (from Assignment 1)
UC1 event propagation; UC2 timely fraud/AML data; UC3 consistent multi-system state; UC4 fault-tolerant decoupling; UC5 audit and lineage; UC6 comparing CDC vs batch latency.
Dashboards per docs/demo-flow.md: D1 Account Opening Growth (UC1), D2 Onboarding Risk & Compliance (UC2), D3 Customer Engagement & Consistency (UC3). UC4 is shown by the optional resilience scene. UC5 is covered by run_id/raw/vault lineage. UC6 has no dashboard in the spec.

## UC3 reconciliation design *(superseded)*
docs/demo-flow.md replaces the reconciliation demo and dashboard with the D3 consistency-check view: declined but enrolled; declined but ACTIVE in CRM; approved but missing in CRM; approved + consented but not enrolled. All are expected to be 0. The earlier design is NOT in the current build: recon_sla/recon_run/recon_break, IN_FLIGHT/OPEN/AUTO_REPAIRED/RESOLVED, and emit_account_snapshot repair.

## Dashboards, synthetic data and demo script *(superseded)*
See docs/demo-flow.md sections 3, 6 and 7. The earlier Metabase/Grafana dashboards, the "5-10% rejections" data notes and the recon-based demo steps no longer apply.

## Known lab simplifications (state in report)
- Single PostgreSQL container, with postgres_fdw between databases.
- One superuser for every component (no least-privilege roles, which production would have).
- Single Kafka broker (replication factor 1).
- Rules-based fraud score instead of an ML model; the fraud decision is applied to core_banking by a simulated step.
- No Schema Registry (a versioned JSON contract instead).
- Production would use: separate servers, RF=3, min.insync.replicas=2, max_slot_wal_keep_size + slot-lag monitoring.
