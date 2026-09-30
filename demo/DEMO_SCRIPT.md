# West City Bank – demo script (about 5 minutes)

The video follows ONE new account from the mobile app, through every system,
into the live dashboard (docs/demo-flow.md section 7). Every command runs
from the project folder.

| File | Scene | What it does |
|---|---|---|
| `demo/01_open_account_mia.sql` | 1 | Mia opens an EVERYDAY account (expected: APPROVE, 5,000 points) |
| `demo/apply_decisions.sql` | 2 | Step 2b: apply the fraud decision (APPROVE → ACTIVE, DECLINE → REJECTED) |
| `demo/03_trace_account.sql` | 2, 3 | Follow one account through core → Kafka → fraud → CRM → loyalty → warehouse |
| `etl/run_all.sql` | 3 | Warehouse load: extract → bronze → silver (Data Vault) → gold (star) |
| `demo/02_open_account_alex_fraud.sql` | 5 | Alex, a fraudster: fraud-ring device + KYC FAIL (expected: DECLINE) |
| http://localhost:8050 | 0, 4, 5 | Live dashboards; they re-read the warehouse every 5 seconds |

---

## Before recording (once, about 5 minutes)

1. **Enough history for the charts.** In `seed/seed_history.sql` set
   `\set seed_count 2000` (the spec asks for about 2,000 accounts; with 10
   the charts are nearly empty and Alex's fraud ring is too small).

2. **Start from zero:**
   ```
   docker compose down
   docker volume rm westcity_pgdata westcity_kafkadata
   docker compose up -d --build
   ```
   Wait until `docker compose ps` shows postgres, kafka and connect as **healthy**.

3. **Load the 90-day history.** It goes through the real pipeline, like the demo accounts:
   ```
   docker compose exec -T postgres psql -U westcity_admin -d core_banking < seed/seed_history.sql
   ```
   Wait about 10 seconds, until the fraud consumer has scored them all, then:
   ```
   docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql > /dev/null
   ```
   Wait about 10 more seconds, until CRM, Loyalty and the warehouse consumer catch up, then:
   ```
   docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/run_all.sql
   ```

4. **Arrange the screen:**
   - **Browser** (left half): http://localhost:8050, tab D1. It should show
     *accounts opened today 0* and about 2,000 accounts in the warehouse.
   - **Terminal A** (top right): the commands below.
   - **Terminal B** (bottom right): the consumers' live log. Leave it running:
     ```
     docker compose logs -f --since 1s consumer-fraud consumer-crm consumer-loyalty consumer-dw
     ```

---

## Scene 0 – Before (30 s)

**Show:** the D1 tab. Point at *accounts opened today = 0*, the 90-day line,
and the status line (*warehouse last loaded …*).

**Say:** "This is West City Bank's account-opening dashboard. It reads the
data warehouse and refreshes every 5 seconds. No accounts have been opened
today yet."

## Scene 1 – Mia opens an account (45 s)

**Terminal A:**
```
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/01_open_account_mia.sql
```

**Show:**
- Terminal A: Mia's account is PENDING, and the transaction wrote two events
  to the outbox: *CustomerCreated* and *AccountOpened*.
- Terminal B: within a second, all four consumers react to *AccountOpened*:
  - `[fraud] … scored: NN LOW -> APPROVE`
  - `[crm] … applied: YOUNG_PROFESSIONAL, UNDER_REVIEW`
  - `[loyalty] … not needed here` (Loyalty waits for the account to be approved)
  - `[dw] … landed in bronze.core_event`

**Say:** "Core banking saves the account and, in the same transaction, an
event in its outbox. Debezium reads the outbox from the database log and
publishes the event to Kafka. The fraud engine, CRM, loyalty and the
warehouse each read it independently. The customer never waits for them."

## Scene 2 – Follow the event (60 s)

**Terminal A:** apply the fraud decision (step 2b):
```
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql
```
**Terminal B:** *AccountActivated* reaches CRM (`NEW_ONBOARDING`) and Loyalty
(`enrolled: BRONZE, welcome offer 5000 points`).

**Terminal A:** trace Mia's account through every system (the newest account
by default):
```
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/03_trace_account.sql
```
**Show, top to bottom:**
1. core banking: ACTIVE, version 2, and three events in the outbox;
2. Kafka: topic, partition and offset of each event;
3. fraud: the signals it checked, then score, LOW, APPROVE, processed about 0.5 s after the event;
4. CRM: YOUNG_PROFESSIONAL, NEW_ONBOARDING, WELCOME_CALL task OPEN;
5. loyalty: ENROLLED, BRONZE, 5,000 points;
6. warehouse: still empty, because it hasn't been loaded yet.

**Say:** "Each system added only what it owns: a risk decision, a segment and
a welcome task, a loyalty membership. The *seconds_after_written* column shows
each one received the event in under a second."

## Scene 3 – Load the warehouse (45 s)

**Terminal A:**
```
docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/run_all.sql
```
**Show:** three runs (extract, silver, gold), each SUCCEEDED, with their row counts.

Optionally, re-run the trace and scroll to section 6. It now shows Mia's
status history in silver and her one combined row in gold.

**Say:** "The warehouse extracts only new and changed rows into bronze. It
builds a Data Vault in silver that keeps every version, then a star schema in
gold that the dashboards read."

## Scene 4 – After (30 s)

**Show the browser.** No reload is needed: within 5 seconds *accounts opened
today* = 1 and *approved today* = 1, the status line shows the new load time,
and Mia is at the top of *Latest 15 openings*.

Show that it's interactive: hover a bar for its value, drag to zoom the
90-day line, double-click to reset, click a legend item to hide a series.

## Scene 5 – A fraudster (60 s)

**Terminal A:**
```
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/02_open_account_alex_fraud.sql
```
It prints which fraud-ring device Alex used and how many customers had used
it before him.

**Terminal B:** `[fraud] … -> DECLINE (SHARED_DEVICE,KYC_FAIL)`.

**Terminal A:** apply the decision, then load the warehouse:
```
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql
docker compose exec postgres psql -U westcity_admin -d westcity_dw -f /etl/run_all.sql
```
**Show in the browser:**
- D1: *declined today* = 1.
- D2: in *Devices shared by 3+ customers*, Alex's device has one more
  customer and one more decline. *Top risk reason codes* now includes his.
- D3: all four consistency checks are still 0. He was declined everywhere:
  REJECTED in core, DECLINED in CRM, not enrolled in loyalty.

**Say:** "The fraud engine declined Alex because his KYC failed, and flagged
the shared device. Every system agrees he was declined."

## Scene 6 – Optional: a system goes down (60 s)

**Terminal A:**
```
docker compose stop consumer-loyalty
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/01_open_account_mia.sql
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/apply_decisions.sql
```
(The Mia script opens a new account for another "Mia" each time it runs.)

**Show:**
- Terminal B: fraud, CRM and the warehouse consumer still work; there are no loyalty lines.
- Kafka keeps loyalty's unread events:
  ```
  docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group loyalty-consumer
  ```
  One partition shows **LAG 2**.

**Terminal A:**
```
docker compose start consumer-loyalty
docker compose exec -T postgres psql -U westcity_admin -d core_banking < demo/03_trace_account.sql
```
**Show:** Terminal B shows loyalty catching up. In the trace, section 5 has
the new account ENROLLED, processed several seconds after the event was
written: the time loyalty was down.

**Say:** "When loyalty was down, nothing else stopped. Kafka kept its events,
and when it came back it caught up. Nothing was lost and nothing was applied
twice."

---

## If something goes wrong

| Symptom | Fix |
|---|---|
| The dashboard does not change | It shows the **warehouse**, so run `etl/run_all.sql` first. The page refreshes within 5 s. |
| `apply_decisions.sql` returns 0 rows | The fraud consumer has not scored the account yet (check Terminal B); run it again. |
| The trace shows `-` for fraud, CRM or loyalty | That consumer has not processed the event yet (or does not need it); wait a second and run it again. |
| Alex's script prints a NOTE about the ring | Too little history: use `seed_count 2000` and rebuild. |
| http://localhost:8050 does not open | `docker compose ps dashboard`, then `docker compose logs dashboard`. |
| Clean retake | Repeat "Before recording" steps 2–3 (about 3 minutes). |

Backup: the same charts as static files, from
`docker compose exec python python /workspace/dashboards/build_dashboards.py`
(open `dashboards/output/*.html`).
