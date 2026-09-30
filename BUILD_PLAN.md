# West City Bank Prototype – Build Plan

Step-by-step plan for building the Assignment 2 prototype (UTS 32113). It sits on top of [CLAUDE.md](CLAUDE.md), which remains the source of truth for architecture and conventions. Full table definitions are in [westcity-er-model-source.md](westcity-er-model-source.md).

- **Due:** 11:59pm Friday 16 October 2026
- **Plan written:** 29 September 2026

> **⚠ Restarted from scratch (29 Sep, evening).** All infrastructure was deleted and is being rebuilt step by step, as simply as possible. That means no Makefile, `.env`, helper scripts, `db-migrate` or roles.
>
> **Current state:** `docker-compose.yml` has Postgres only, and `sql/01_create_databases.sql` creates the 5 databases through Postgres's init folder.
>
> **The "Done" notes under Steps 1–2 below describe the deleted first attempt.** They're kept only as reference. The design decisions and the Step 3+ content still apply.

> **Prototype v1 scope (revised 29 Sep):** build the source databases, the pipelines and the warehouse first. That means Steps 3–8, then the Step 10 tests and demo.
>
> - **Deferred:** dashboards and reports (Step 9 and `sql/05_reports`). The brief still requires at least 3, so they come back after v1 works end to end.
> - **Simplified:** the database uses one admin user for everything, with no per-service roles.

---

## Decisions already made

| Topic | Decision |
|---|---|
| Kafka → target databases | Python consumers for fraud, CRM, loyalty and the DW lander (no Kafka Connect JDBC sink) |
| Dashboards | **Metabase** for business reports and **Grafana** for technical reports. **Deferred until after prototype v1.** |
| Database access (v1) | One admin user (`POSTGRES_ADMIN_USER`) for every component; no per-service roles |
| Docker Compose | Replace the course lab compose file with our stack only. Drop neo4j, clickhouse, cloudbeaver and the lab python container. |
| 90-day synthetic history | Seed every system and the warehouse directly, with synthetic back-dated latencies. The live demo runs on top of that history. |
| Time zone | `Australia/Sydney`. A1 places West City in Western Australia, so the report must say we use Sydney as head-office time. |
| New event | Add `CustomerCreated`, and add `account.onboarding_session_id` (a logical reference to `identity_fraud`) |
| Git | Managed manually by the team. `ops.etl_run.script_version` is a hard-coded version label in the ETL script (e.g. `'v1'`); no git lookup, to keep it simple. |
| Postgres version | postgres:15, the same version as the course lab environment |

---

## Step 0: Update the docs (before any code) ✅ Done 29 Sep 2026

Updated CLAUDE.md, the ER model, and new [docs/event-contract.md](docs/event-contract.md).

Beyond the list below, writing the contract settled four more points:
- **`outbox.event_version`** is the aggregate version (drives the versioned upsert). The contract's schema version is `schema_version` inside the payload.
- **Every account event embeds its holders' customer snapshots**, because there is no ordering between the customer and account topics.
- **`recon_break.repair_requested_at`** distinguishes RESOLVED (a manual repair) from AUTO_REPAIRED.
- **`ops.consumer_health`** was added to the ER model.

The original Step 0 list:

- **Events:** add `CustomerCreated`; add `account.onboarding_session_id`.
- **Recon SLAs:** `ops.recon_sla` gets FRAUD and DW rows as well as CRM and LOYALTY, because `fact_event_propagation.within_sla` needs an SLA for every target.
- **`dim_customer`:** `valid_from`/`valid_to` become `timestamptz`, so two changes on the same day don't collide.
- **`AccountSnapshot` repair:** it must bump the version in `core_banking` too, not only in the event. Otherwise the target ends up ahead of core, and the STALE check (target version < core version) misreads it.
- **CRM MISMATCH check:** email and mobile live on `crm_contact`, a customer-level record. So the CRM MISMATCH hash joins account → holder → customer, and a snapshot repair also bumps the customer version.
- **Account opening:** happens in one `open_account()` call. `AccountOpened` is emitted by a *deferred* constraint trigger that runs at commit, once `account_holder` rows exist.
- **Loyalty compensation:** `AccountRejected` only unenrols a member who has no other eligible account.
- **Joint accounts:** `fact_account_opening.customer_key` is the PRIMARY holder.
- **Stack choices:** record the decisions table above.
- **New section: event contract v1.** It defines the JSON payload fields for every event type, so all consumers and the warehouse agree on one format.

---

## Step 1: Docker Compose

### Containers

| Service | Image | Purpose |
|---|---|---|
| `postgres` | postgres:15 | All 5 databases. Flags: `wal_level=logical`, enough replication slots and WAL senders, a cap on retained WAL (`max_slot_wal_keep_size`), `timezone=Australia/Sydney`. Port `5433:5432`. |
| `kafka` | apache/kafka (KRaft mode, no ZooKeeper) | Single broker, replication factor 1. Separate listeners for other containers and for the host. |
| `kafka-init` | kafka image, runs once | Creates topics with fixed partition counts: `bank.account.events`, `bank.customer.events`, `bank.dlq` (failed messages). |
| `connect` | quay.io/debezium/connect | Kafka Connect with Debezium. JSON messages without embedded schemas. |
| `connect-init` | curl, runs once | Waits for Connect to be healthy, then registers the 4 connectors from `connectors/*.json`. Safe to rerun (it updates rather than duplicates). |
| `db-migrate` | postgres:15, runs once | Runs `psql` with stop-on-first-error over `sql/` in numbered order, so "runs top to bottom without errors" is checked on every startup. |
| `consumer-fraud`, `consumer-crm`, `consumer-loyalty`, `dw-lander` | one shared Python image | One container per consumer, so a single consumer can be stopped for the demo. |
| `etl-runner` | same Python image | Every N seconds, calls the SQL procedures that load the warehouse and run reconciliation. The logic stays in SQL. |
| ~~`metabase`~~ | metabase/metabase | *Deferred (Step 9).* Business dashboards, port 3000 |
| ~~`grafana`~~ | grafana/grafana-oss | *Deferred (Step 9).* Technical dashboards, port 3001 |
| `kafka-ui` | kafbat/kafka-ui | Shows topics, messages, consumer lag and connectors. Used for pipeline debugging and the demo video. |

All images are pinned to exact versions and must run on Apple Silicon (arm64). Docker needs about 6–8 GB of RAM.

### Startup order

Healthchecks plus `depends_on` (`service_healthy` / `service_completed_successfully`) give this order:

postgres → db-migrate → kafka → kafka-init → connect → connect-init → consumers → etl-runner and the dashboards

### Other files

- **`.env`:** ports, the admin user and password.
- **`Makefile`:** `up`, `down`, `reset` (`down -v` and rebuild), `status`, `logs`, `psql-<db>`, `decide`, `demo-1` … `demo-6`, `test`.
- **`docker/postgres/`:** Postgres config plus the init script that creates databases and roles.

### Consumer health without Prometheus

- Consumers write their committed offset and the topic's end offset to `westcity_dw.ops.consumer_health`.
- Outbox backlog is read from `pg_replication_slots` on `core_banking`.

### Done when

- `make up` on an empty machine reaches all-healthy.
- Connect lists the Postgres connector plugin.
- `db-migrate` exits 0.

### ✅ Done 29 Sep 2026

**Files:** `docker-compose.yml`, `.env`, `Makefile`, `.gitignore`, `docker/postgres/postgresql.conf`, `scripts/{db-migrate.sh,create-topics.sh,register-connectors.sh}`.

**Pinned images** (dashboard images kept here for when Step 9 resumes):

| Image | Version |
|---|---|
| postgres | `15.18` |
| apache/kafka | `4.3.1` |
| quay.io/debezium/connect | `3.6.3.Final` |
| kafbat/kafka-ui | `v1.5.0` |
| curlimages/curl | `8.22.0` |
| metabase/metabase *(deferred)* | `v0.63.18.4` |
| grafana/grafana-oss *(deferred)* | `13.0.2` |

**Verified on this Mac** (Docker 28.3, 8 GB):

- `make reset` exits 0 in about 25 s, with every long-running service healthy and every one-shot exited 0.
- **Postgres:**
  - `wal_level=logical`;
  - Sydney time zone;
  - the single `westcity_admin` user logs in over the Docker network with its password.
- **Debezium:** the Postgres connector plugin is loaded.
- **Kafka:** the 3 topics exist.
- **Kafka UI** responds.

**Revised for v1 (29 Sep):**
- Metabase, Grafana, `dashboards/` and the custom `pg_hba.conf` were removed; the image's default authentication is enough with one superuser.
- Connect reads `${env:DB_USER}` / `${env:DB_PASSWORD}`, both set to the admin user.
- **Scripts simplified, with values hard-coded:**
  - **`db-migrate.sh`** runs every `sql/0*/*.sql` file in order on a fresh database, stopping at the first error. It then creates `public.setup_done`. On later starts it sees that table and skips. `99_demo` is never run.
  - Any SQL change or new SQL file means `make reset`.
  - The checksum tracking table, `make migrate` and `GIT_COMMIT` were removed.
  - Verified: a fresh `make reset` runs the scripts; a second `make up` skips them.

**Differences from the plan above:**

- **SQL folders renumbered so the number is the execution order:** `02_warehouse`, `03_etl`, `04_recon`, `05_reports`, `06_seed_data`. Seed data moved last because the backfill needs the warehouse, ETL and reports.
- **No Postgres init script.** Databases and roles come only from `sql/00_setup`, run by `db-migrate`.
- **Consumers and `etl-runner` are not in Compose yet.** They're added in Step 8, when their code exists; a placeholder container would only fail.
- **`.env` is committed on purpose** (lab-only passwords), so `make up` works with no setup.
- **Data lives in named Docker volumes, not project folders.** The project sits in OneDrive, which would corrupt live database files.
- **`kafka-ui` waits for `connect-init` to finish.** This gives the one-shot a dependent, which `docker compose up --wait` needs to accept its normal exit.
- **Connector files** are `connectors/<name>.json` holding only the config map. Credentials are referenced as `${env:DB_USER}` / `${env:DB_PASSWORD}` (resolved by Connect's `EnvVarConfigProvider`), never stored in the JSON.
- **Make targets now:** `up`, `up-core`, `down`, `reset`, `status`, `logs s=<svc>`, `psql db=<db>`, `topics`, `connectors`, `connectors-status`. The `decide`, `demo-N` and `test` targets come with their steps.

---

## Step 2: Setup scripts (`sql/00_setup/`)

1. **Databases:** `core_banking`, `identity_fraud`, `crm`, `loyalty`, `westcity_dw`, all set to `Australia/Sydney`.
2. **Extensions:**
   - `postgres_fdw` in `westcity_dw`, used for reconciliation and the simulated nightly batch;
   - `dblink` in `core_banking`, for the single simulated fraud-decision UPDATE.

   Both are lab simplifications, and neither adds a cross-database foreign key.
3. **Roles:** none in v1. Everything uses the admin user from `.env`, which is a superuser, so it can also do Debezium's logical replication.

### ✅ Done 29 Sep 2026 (simplified for v1 the same day)

Two files: `01_databases.sql`, `02_extensions.sql`. Both are idempotent and re-run on every `db-migrate`. Verified by `make reset` from zero.

*The first version also had per-service roles (`01_roles.sql`, `04_privileges.sql`) with a least-privilege access matrix. It was removed to keep v1 simple; the files were deleted, so restoring it later means rewriting them.*

### What this means for later steps

- **Step 3:** create each Debezium publication explicitly in SQL. For `core_banking` it covers only `outbox`.
- **Step 7:** connectors use `publication.autocreate.mode=disabled`, since publications are created in SQL. This keeps "publication = outbox only" enforced in versioned SQL.

---

## Step 3: Source database scripts (`sql/01_create_sources/`)

One file per database, fully commented, with CHECK constraints for every enumerated value.

### `core_banking`

- **Tables:** the 6 tables from the ER model, plus `onboarding_session_id`, plus the outbox.
  - The outbox uses Debezium's default columns plus `event_version`.
  - It also gets a `tx_id` column filled with `txid_current()`, so outbox events carry `_src_tx_id` for lineage.
- **Version trigger:** a BEFORE UPDATE trigger increments `version` when business columns change.
- **Outbox triggers:**
  - `customer` insert → `CustomerCreated`; update → `CustomerDetailsUpdated`
  - `account` insert → `AccountOpened` (deferred constraint trigger, runs at commit)
  - `account` status change → `AccountActivated` / `AccountRejected`
- **Functions:**
  - `open_account(...)` creates the customer (if new), the account and its holders in one transaction.
  - `emit_account_snapshot(account_id)` bumps the versions and writes an `AccountSnapshot` event.
- **Publication:** a Debezium publication on `core_banking.outbox` **only**.

### `identity_fraud`, `crm`, `loyalty`

- **Tables:** as in the ER model, each database with its own `processed_event`, plus indexes on `customer_id`/`account_id`.
- **Publications:** each database gets one covering the tables captured by CDC, including `processed_event`, because that's where `applied_ts` comes from for latency.
- **Reference data:** `loyalty_offer` rows.

---

## Step 4: Warehouse scripts (`sql/02_warehouse/`)

Created in this order: `ops` → `staging` → `raw_vault` → `mart`.

- **ops:** `etl_run`, `dq_quarantine`, `recon_sla` (CRM 5 min, LOYALTY 15 min, plus FRAUD and DW, with the demo override), `recon_run`, `recon_break`, `consumer_health`.
- **staging:**
  - `stg_outbox_event` for landed business events, plus one `stg_cdc_<db>_<table>` per captured table.
  - All carry the metadata columns `_src_system`, `_src_table`, `_src_lsn`, `_src_tx_id`, `_src_commit_ts`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_op`, `_load_ts`, `_run_id`.
  - A unique constraint on (topic, partition, offset) means replayed messages can't create duplicates.
- **raw_vault:** hubs, links and satellites as in the ER model. Helper functions for:
  - MD5 hash keys and `hash_diff`;
  - normalisation: trim, lower-case email, E.164 phone, COALESCE nulls, status mapping.
- **mart:** dimensions and facts from the ER model; `dim_date` is populated for the full date range.

---

## Step 5: ETL, reconciliation and report scripts

### `sql/03_etl/`

- **`ops.start_run()` / `ops.finish_run()`:** every load opens an `etl_run` row and stamps its `run_id` on every row it writes.
- **`load_raw_vault(run_id)`:** staging → hubs, links and insert-only satellites (a new row only when `hash_diff` changes). Rows that fail a data quality rule go to `dq_quarantine` with a reason.
- **`load_mart(run_id)`:** SCD2 `dim_customer`, the other dimensions, `fact_account_opening`, `fact_event_propagation`.
  - Latency is `applied_ts − event_created_ts`.
  - `applied_ts` comes from each target's `processed_event.processed_at`, or for the DW, the staging `_load_ts`.
- **`batch_extract_nightly(run_id)`:** a pure-SQL extract through `postgres_fdw` into staging, tagged `BATCH`. It serves two purposes:
  - the simulated nightly batch for the UC6 comparison with CDC;
  - the explicit SQL "extract" script the brief asks for.

### `sql/04_recon/`

- **`run_recon()`:** reads live `core_banking`, `crm` and `loyalty` through `postgres_fdw`.
  - Checks MISSING, STALE and MISMATCH (MD5 of normalised fields) for each account and target.
  - Classifies each break IN_FLIGHT or OPEN against the SLA; breaks that no longer appear become AUTO_REPAIRED.
- **`resolve_break()`:** marks a break RESOLVED after a manual fix.

### `sql/05_reports/` *(deferred until after prototype v1)*

- One view per dashboard panel. Metabase and Grafana only run `SELECT` against these views, so all report logic lives in versioned SQL.

---

## Step 6: Synthetic data (`sql/06_seed_data/`)

All randomness uses a fixed seed (`setseed`), so every rebuild produces the same data.

1. **Reference data:** branches (6-digit BSB), 5 products, channels.
2. **`core_banking` history:** about 2,000 accounts over 90 days with 5–10% rejections and varied activation delays.
   - Triggers are switched off for the session (`session_replication_role = replica`), so history creates **no outbox rows and no Kafka traffic**.
3. **`identity_fraud` history:** devices, sessions, KYC, watchlist screening and risk assessments, including planted fraud rings (e.g. 6 customers on one device).
4. **`crm` / `loyalty` history:** built from core using the same rules the consumers apply, with `source_version` equal to the core version, so reconciliation starts clean. A few deliberate historical breaks are planted for the consistency dashboard.
5. **`processed_event` history** in each target, with back-dated `processed_at`. This gives realistic p50/p95 latency history, with a small share over the SLA.
6. **Warehouse backfill:** `batch_extract_nightly` → `load_raw_vault` → `load_mart`, run under an `etl_run` with job_name `BACKFILL`.
7. **Reset ID sequences** past the seeded ranges, so live demo accounts continue cleanly.

**Keeping history out of Kafka:** the CDC connectors use `snapshot.mode=no_data`, so seeded history isn't pushed through Kafka and duplicated in staging. Only live changes flow.

---

## Step 7: Connectors (`connectors/`)

### `core-outbox.json`

- `pgoutput` plugin, `publication.autocreate.mode=disabled` (the publication is created in SQL in Step 3), `table.include.list=core_banking.outbox`.
- Outbox EventRouter transform:
  - route by `aggregatetype`;
  - rename topics to `bank.${routedByValue}.events`;
  - message key = `aggregateid`;
  - `type`, `event_version` and `tx_id` go in message headers.
- Tombstones off; a heartbeat keeps the replication slot moving when idle.

### `identity-fraud-cdc.json`, `crm-cdc.json`, `loyalty-cdc.json`

- Topic prefix `cdc.<db>`.
- The ExtractNewRecordState transform adds `op`, `lsn`, `txId`, `source.ts_ms` and the table name. The lander maps these to the staging metadata columns.

Each connector has its own named replication slot and publication.

---

## Step 8: Python consumers (`consumers/`)

Python 3.12, `confluent-kafka` and `psycopg` 3.

### `common/` (shared by all consumers)

- Config from environment variables.
- The processing loop runs with auto-commit off. For each message:
  1. Begin a DB transaction.
  2. Insert into `processed_event` with ON CONFLICT DO NOTHING. If no row was inserted, the event was already processed: skip it.
  3. Run the handler (versioned upsert: `... WHERE excluded.source_version > target.source_version`).
  4. Commit the DB transaction.
  5. Commit the Kafka offset.

  A crash between steps 4 and 5 is harmless: the message is redelivered and step 2 skips it. This is at-least-once delivery.
- Retries with backoff; messages that still fail go to `bank.dlq`.
- Writes to `ops.consumer_health`; shuts down cleanly on stop.

### Handlers

- **`crm`:** `CustomerCreated`, `CustomerDetailsUpdated` and `AccountSnapshot` update `crm_contact`; account events update `crm_financial_account`.
- **`loyalty`:** members and account links.
  - A member is eligible only when the account is ACTIVE and marketing consent is true.
  - `AccountRejected` unenrols the member, but only if they have no other eligible account.
- **`fraud`:** a rules-based score (`model_version = rules-v1`) from:
  - how many customers share the device;
  - KYC result;
  - watchlist status;
  - account-opening velocity.

  The score maps to APPROVE/REVIEW/DECLINE and is written to `risk_assessment`. The fraud consumer never touches `core_banking`.
- **`dw_lander`:** subscribes to `bank.*` and `cdc.*` and appends raw rows plus metadata to staging.

### Simulated fraud decision

`make decide` runs the single simulated UPDATE in `core_banking`:
1. It reads decisions through `dblink` and moves PENDING accounts to ACTIVE or REJECTED.
2. The trigger emits `AccountActivated` or `AccountRejected`.
3. Those events flow back out to CRM and loyalty.

---

## Step 9: Dashboards (`dashboards/`) *(deferred until after prototype v1)*

When this resumes:
- add the `metabase` and `grafana` services back to Compose (pinned versions are in Step 1);
- recreate Grafana provisioning (data source + dashboard provider);
- write the `sql/05_reports` views first.

### Metabase (business)

1. Account Opening Performance
2. Fraud & AML Onboarding Risk
3. Customer X as at time T (lineage)

Metabase's free edition can't export dashboards as files, so `dashboards/metabase/setup.py` rebuilds them from the `05_reports` views through Metabase's API. Screenshots go in the report.

### Grafana (technical)

Loaded automatically from files:

1. Propagation Latency & Freshness (CDC vs simulated nightly batch)
2. Cross-System Consistency
3. Consumer Health (lag, outbox backlog, replication slot lag)

---

## Step 10: Tests, demo scripts and rehearsal

### `tests/`

SQL assertion scripts that fail loudly if something is wrong. They check:

- row counts in each system;
- no duplicate events;
- every account appears in CRM;
- loyalty eligibility follows the rule;
- vault satellites are only ever inserted, never updated;
- `etl_run` has no failed rows;
- reconciliation is clean after catch-up.

`make test` runs them all.

### `sql/99_demo/` + `make demo-N`

One script per demo step. Each prints the state before and after.

1. Happy path: open 3 accounts; show outbox → Kafka → CRM/loyalty/DW.
2. Stop the loyalty consumer, open 5 accounts → recon shows IN_FLIGHT, then OPEN after the SLA.
3. Restart the consumer → it catches up → AUTO_REPAIRED.
4. Corrupt a CRM email by hand → MISMATCH break.
5. Insert an `AccountSnapshot` → RESOLVED.
6. Reject an account → loyalty unenrols the member.

Plus: stop Debezium, show account opening still works while outbox rows pile up, then resume.

### Final rehearsal

On a clean Docker:

```
make reset && make up && make test && make demo-1 … demo-6
```

It must need no manual steps before we record.

---

## Milestones

| # | Milestone | Done when | Target |
|---|---|---|---|
| M0 | Step 0 docs update | Team has reviewed the updated CLAUDE.md and ER model | 30 Sep |
| M1 | Steps 1–3 (core + crm), outbox connector, CRM consumer | Open 3 accounts → rows appear in CRM; replay and out-of-order tests pass; stopping Debezium doesn't block opening | 2 Oct |
| M2 | Loyalty + fraud consumers, `make decide`, CDC connectors | Activate/reject flows reach CRM and loyalty; `cdc.*` topics have data | 5 Oct |
| M3 | Warehouse DDL, lander, vault + mart ETL | Live events appear in staging → raw_vault → mart with `run_id` stamped | 8 Oct |
| M4 | Seed data + backfill | 90 days of history in every system; reconciliation starts clean | 9 Oct |
| M5 | Reconciliation, UC6 batch comparison (v1); then dashboards (deferred) | Demo steps 2–5 work; dashboards show data once Step 9 resumes | 12 Oct |
| M6 | Tests, demo scripts, clean-machine rehearsal | `make reset` → all green | 13 Oct |
| — | Buffer: record the video, write the report | | 14–16 Oct |

**Why this order:** the CRM path (M1) comes before everything else, so the riskiest part (Debezium outbox → Kafka → idempotent consumer) is proven before we add breadth.

**If time runs short**, cut in this order:
1. Dashboards 4–6
2. UC6 batch comparison
3. Demo step 6

The 3 core dashboards and the end-to-end path are never cut.

Commit after each milestone.

---

## Planned folder structure

```
assignment/
├── docker-compose.yml
├── .env
├── Makefile
├── docker/
│   ├── postgres/          # postgresql.conf
│   └── consumers/         # Dockerfile, requirements.txt (Step 8)
├── sql/                   # folder number = execution order (db-migrate)
│   ├── 00_setup/          # databases, extensions
│   ├── 01_create_sources/ # core_banking, identity_fraud, crm, loyalty
│   ├── 02_warehouse/      # ops, staging, raw_vault, mart DDL
│   ├── 03_etl/            # batch extract, staging→vault, vault→mart
│   ├── 04_recon/          # checks, classification, repair
│   ├── 05_reports/        # one view per dashboard panel (deferred)
│   ├── 06_seed_data/      # reference data, 90-day history, fraud rings, backfill (needs 02–05)
│   └── 99_demo/           # one script per demo step (never run automatically)
├── connectors/            # 4 Debezium connector configs (<name>.json = config map only)
├── consumers/             # common/, fraud/, crm/, loyalty/, dw_lander/
├── dashboards/            # deferred: metabase/ (setup.py), grafana/ (provisioning + JSON)
├── scripts/               # db-migrate.sh, create-topics.sh, register-connectors.sh
├── tests/                 # SQL assertion scripts
└── docs/                  # runbook, event contract, diagrams, lab simplifications
```
