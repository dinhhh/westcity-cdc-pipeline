# West City Bank – Event-Driven Integration Prototype (UTS 32113 Assignment 2)

## Project context
- Group assignment for UTS 32113 Advanced Database. Due 11:59pm Friday 16 October 2026.
- Case: West City Bank, a customer opening a new transaction account in the mobile app. The account-opening event must reach the fraud engine, CRM, loyalty and the data warehouse reliably, without blocking the customer.
- Scope is ACCOUNT OPENING only. Money transfers are out of scope (optional UC2 extension only if the core works end to end).
- A non-working prototype scores zero, so reliability and reproducibility come before features.
- Deliverables for the prototype: at least 3 source databases, at least 1 integrated warehouse, fully commented SQL scripts with synthetic data (create, extract, transform, load) that run without errors, at least 3 reports/dashboards, and a recorded end-to-end demo.
- Build order, milestones and file layout: BUILD_PLAN.md. Event payloads: docs/event-contract.md.

## Mandatory stack
- PostgreSQL 15 (all source databases and the warehouse; same version as the course lab)
- Apache Kafka (event transport, KRaft mode)
- Debezium (change data capture, via Kafka Connect)
- Everything runs with Docker Compose (separate containers: postgres, kafka, kafka-connect/debezium). Not a single Dockerfile.
- RESTARTED FROM SCRATCH on 29 Sep 2026, building step by step. Current state (30 Sep):
  - Compose services: postgres, cloudbeaver (web SQL editor, localhost:8978), kafka (KRaft, single broker), connect (Debezium 3.6.3), and connect-init (a one-shot curl that registers connectors/core-outbox.json).
  - SQL: 01_create_databases, 02_create_tables, 03_outbox_triggers (triggers + publication dbz_outbox_publication).
  - Working and verified: outbox -> Debezium EventRouter -> topics bank.customer.events / bank.account.events. Key = aggregateid; headers id, type, event_version, tx_id; 3 partitions.
  - Consumers and the warehouse loading are next.
- Run: `docker compose up -d`. Rebuild the data: `docker compose down && docker volume rm westcity_pgdata westcity_kafkadata && docker compose up -d`. Reset Postgres and Kafka together, because Debezium's saved position must match the database.
- SQL scripts live FLAT in sql/ (e.g. sql/01_create_databases.sql, sql/02_...). The folder is mounted at /docker-entrypoint-initdb.d.
  - Postgres runs them in name order, ONLY when the data volume is empty, and stops at the first error.
  - Any SQL change requires a rebuild from zero.
  - Subfolders are ignored.
- Keep everything simple: hard-coded values (host, user, password), no .env, Makefile or helper scripts unless the user asks.
- ONE database user for everything: westcity_admin / lab_admin (lab only), host port 5433. No per-service roles or grants.

## Implementation decisions (agreed 29 Sep 2026)
- Kafka -> target databases: small Python consumers (confluent-kafka + psycopg 3) for fraud, CRM, loyalty and the DW lander. No Kafka Connect JDBC sink (it cannot write processed_event in the same transaction or apply the version guard).
- PROTOTYPE V1 SCOPE (29 Sep): source databases, pipelines (outbox/CDC -> Kafka -> consumers) and the warehouse only.
  - Dashboards and reports (Metabase business, Grafana technical, views in sql/05_reports) are DEFERRED, not dropped. The brief still requires at least 3.
  - No dashboard containers in docker-compose.yml until then.
- docker-compose.yml contains our stack only (target: postgres, kafka, connect, consumers). The course lab services neo4j, clickhouse, cloudbeaver and python are not used.
- Synthetic history (90 days) is seeded directly into every system and the warehouse with back-dated latencies; it never passes through Kafka. The live demo runs on top of it.
- Git is managed manually by the team. ops.etl_run.script_version is a hard-coded version label in the ETL script (e.g. 'v1').
- A1 places West City in Western Australia; we still use Australia/Sydney as head-office business time. State this in the report.

## Architecture
```
core_banking.customer/account --(trigger, same transaction)--> core_banking.outbox
  --> Debezium outbox event router --> Kafka topics bank.customer.events / bank.account.events
      (key = customer_id / account_id)
      --> identity_fraud consumer (risk scoring)
      --> crm consumer (versioned upsert)
      --> loyalty consumer (versioned upsert)
      --> dw lander --> westcity_dw.staging (landed events)
identity_fraud, crm, loyalty tables --> Debezium table CDC --> Kafka cdc.* topics --> dw lander --> westcity_dw.staging
westcity_dw: staging --> raw_vault (Data Vault 2.0) --> mart (star schema); ops (audit + reconciliation)
Risk decision back to core_banking is SIMULATED with one SQL UPDATE in the prototype.
```
All five databases can live in one PostgreSQL container (one Debezium connector per database). This is a stated lab simplification.

## Databases and tables (full column lists: westcity-er-model-source.md)
### core_banking (system of record)
customer, customer_address, branch (6-digit BSB), product, account (status PENDING/ACTIVE/REJECTED/CLOSED, channel MOBILE/BRANCH/WEB, version, onboarding_session_id), account_holder (M:N, PRIMARY/JOINT), outbox.
- account.onboarding_session_id is a LOGICAL reference to identity_fraud.onboarding_session (no FK). The fraud consumer uses it to find the device and session.
- Outbox columns use Debezium defaults: id (uuid), aggregatetype, aggregateid, type, payload (jsonb), plus event_version, tx_id and created_at.
  - event_version = the aggregate's version after the change (drives the consumers' versioned upsert). The contract's schema version lives inside the payload.
  - tx_id = txid_current(), so outbox events carry _src_tx_id for lineage.
- Accounts are opened through one function, open_account(), which creates the customer (if new), the account and its holders in one transaction.
- Triggers on customer/account write the outbox row in the SAME transaction and increment `version` when business columns change:
  - customer INSERT -> CustomerCreated; customer UPDATE -> CustomerDetailsUpdated.
  - account INSERT -> AccountOpened, from a DEFERRED constraint trigger that runs at commit, so the account_holder rows already exist.
  - account status -> ACTIVE / REJECTED -> AccountActivated / AccountRejected.
- emit_account_snapshot(account_id) bumps account.version AND the holders' customer.version, then writes an AccountSnapshot event. Version-only bumps do not fire their own events.
- Debezium publication must include ONLY core_banking.outbox (not the whole database).
- Event types: CustomerCreated, CustomerDetailsUpdated, AccountOpened, AccountActivated, AccountRejected, AccountSnapshot (for repair). No AccountClosed event (closing is out of scope).

### identity_fraud
device, onboarding_session, kyc_verification (document_type + document_token hash only, never raw ID numbers), watchlist_screening (SANCTIONS/PEP), risk_assessment (risk_score 0-100, decision APPROVE/REVIEW/DECLINE), processed_event.

### crm
crm_contact (customer_id UK), crm_financial_account (account_id UK), crm_case (CRM-owned), processed_event.

### loyalty
loyalty_member (customer_id UK), loyalty_account_link (account_id PK, eligible), loyalty_offer, member_offer (M:N), processed_event.
- Rule: eligible/enrolled only if account_status = ACTIVE AND marketing_consent = true.
- AccountRejected unenrols the member (compensation), but only if they have no other eligible account. A consent change in CustomerDetailsUpdated re-evaluates all of the member's links.

### westcity_dw
- staging: one table per captured source table/topic, plus metadata columns: _src_system, _src_table, _src_lsn, _src_tx_id, _src_commit_ts, _kafka_topic, _kafka_partition, _kafka_offset, _op, _load_ts, _run_id. Unique on (_kafka_topic, _kafka_partition, _kafka_offset) so replays cannot duplicate rows.
- raw_vault: hub_customer, hub_account, hub_product, hub_device; link_account_holder, link_account_product, link_customer_device; sat_customer_details, sat_customer_kyc, sat_account_state, sat_account_risk. Hash keys = MD5 of business key. Satellites are INSERT-ONLY with load_dts, hash_diff, record_source, run_id.
- mart: dim_date (YYYYMMDD key), dim_customer (SCD2 with timestamptz valid_from/valid_to), dim_product, dim_channel, dim_branch, fact_account_opening (one row per account; customer_key = PRIMARY holder), fact_event_propagation (one row per event per target: FRAUD/CRM/LOYALTY/DW, latency_seconds, within_sla).
  - applied_ts comes from each target's processed_event.processed_at (captured by table CDC). For DW, it is the staging _load_ts.
- ops: etl_run (run_id, script_version = hard-coded label such as 'v1'), dq_quarantine, recon_sla, recon_run, recon_break, consumer_health (per-consumer committed offset, end offset and lag).

## Conventions (apply everywhere)
- Cross-system references are LOGICAL keys (customer_id, account_id, onboarding_session_id), never foreign keys across databases.
- Consumers are idempotent: skip event_id already in processed_event. The processed_event insert, the business upsert and the DB commit happen in one transaction, and the Kafka offset is committed after it.
- Versioned upsert: `INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE excluded.source_version > target.source_version`.
- Delivery is at-least-once; ordering is per key only, and there is NO ordering between the customer and account topics.
  - Therefore every account event embeds a snapshot of each holder's customer (with customer version).
  - Consumers upsert the customer from that snapshot, guarded by version, before touching the account. An AccountOpened that arrives before CustomerCreated still works.
- Every warehouse load opens an ops.etl_run row and stamps run_id on every row it writes; failed rows go to ops.dq_quarantine with a reason.
- SQL scripts must be fully commented and runnable top to bottom without errors.
- Timestamps as timestamptz; business dates in Australia/Sydney.
- The warehouse reads live source state through postgres_fdw only for reconciliation and the simulated nightly batch (read-only, no cross-database FKs; a lab simplification).

## UC3 reconciliation design
- Golden source: core_banking. Targets: crm, loyalty (statements out of prototype scope).
- recon_sla: CRM 5 min, LOYALTY 15 min (set to 1 min for the demo video). FRAUD and DW rows exist for fact_event_propagation.within_sla only; they are not reconciled.
- Checks per account per target:
  - MISSING: no row.
  - STALE: target version < core version.
  - MISMATCH: same version, but the md5 of normalised fields differs.
  - For CRM the compared fields include the holder's contact (email, mobile, consent), joined account -> account_holder -> customer. Contact fields live on crm_contact, not crm_financial_account.
- Classification:
  - age <= SLA -> IN_FLIGHT
  - age > SLA -> OPEN
  - gone on the next run with no manual action -> AUTO_REPAIRED
  - gone after a manual repair (recon_break.repair_requested_at set) -> RESOLVED
- Normalise before hashing: trim, lower(email), consistent phone format (E.164), COALESCE nulls, status code mapping.
- Repair:
  - consumer catch-up from Kafka offsets; or
  - emit_account_snapshot(), which bumps the core versions (so the target does not end up ahead of core) and stamps repair_requested_at on the open breaks.

## Use cases (from Assignment 1)
UC1 event propagation; UC2 timely fraud/AML data; UC3 consistent multi-system state; UC4 fault-tolerant decoupling; UC5 audit and lineage; UC6 comparing CDC vs batch latency.

## Dashboards / reports (DEFERRED until after prototype v1)
Metabase (business):
1. Account Opening Performance (fact_account_opening): openings by day/product/channel, approval vs rejection, time to activation.
4. Fraud & AML Onboarding Risk: high-risk openings, customers sharing a device (1-2 hop SQL, no graph DB).
6. "Customer X as at time T" lineage report.

Grafana (technical):
2. Propagation Latency & Freshness (fact_event_propagation): p50/p95 per target, % within SLA, CDC vs simulated nightly batch.
3. Cross-System Consistency (ops.recon_break): breaks by target/type/severity, in-flight vs open, oldest break.
5. Consumer health: consumer lag (ops.consumer_health), outbox backlog and replication slot lag.

## Synthetic data
- ~2,000 accounts over 90 days via generate_series.
- 4-5 products; 2-3 channels; 5-10% rejections; varied activation delays.
- A few planted fraud rings (e.g. 6 customers on one device).
- Fixed random seed, so every rebuild gives the same data.
- History is seeded with triggers off (session_replication_role = replica) and CDC connectors use snapshot.mode=no_data, so history creates no outbox rows or Kafka traffic.

## Demo script (video)
1. Happy path: open 3 accounts, show outbox -> Kafka -> CRM/loyalty/DW.
2. Stop loyalty consumer, open 5 accounts -> recon shows IN_FLIGHT, then OPEN after SLA.
3. Restart consumer -> catches up -> AUTO_REPAIRED.
4. Manually corrupt a CRM email -> MISMATCH break.
5. Call emit_account_snapshot() (AccountSnapshot outbox event) -> RESOLVED.
6. (Optional) Reject an account -> loyalty unenrols.
Also: stop Debezium, show account opening still works and outbox rows accumulate, then resume.

## Known lab simplifications (state in report)
- Single PostgreSQL container, with postgres_fdw/dblink between databases.
- One superuser for every component (no least-privilege roles, which production would have).
- Single Kafka broker (replication factor 1).
- Simulated fraud decision.
- Rules-based fraud score instead of an ML model.
- No Schema Registry (a versioned JSON contract instead).
- Production would use: separate servers, RF=3, min.insync.replicas=2, max_slot_wal_keep_size + slot-lag monitoring.
