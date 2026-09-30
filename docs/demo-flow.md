# Spec: Demo Story, Data Enrichment Rules and Dashboards

This spec extends CLAUDE.md. Where they conflict, this file wins for the topics it covers:
the demo story, enrichment logic, synthetic data and dashboards.

---

## 1. The story the prototype must tell

Follow ONE account from the mobile app, through every system, into a business dashboard.
Each system ENRICHES the account with information only it owns. The warehouse combines everything
into one record, and the dashboards change visibly during the demo.

| Step | System | Action | Enrichment it adds |
|---|---|---|---|
| 1 | Core Banking | Create customer + account (status PENDING), write `AccountOpened` outbox event | Who and what: customer, product, channel, branch, state, time |
| 2 | Identity & Fraud | Consume event, KYC, device and watchlist checks, score risk, decide | KYC result, risk score, risk band, decision, reason codes, time to decision |
| 2b | Core Banking | Apply decision: APPROVE -> ACTIVE, DECLINE -> REJECTED (emits `AccountActivated` / `AccountRejected`) | Final account status |
| 3 | CRM | Consume events, create contact + account record, segment the customer, create welcome task | Segment, lifecycle stage, welcome-call task |
| 4 | Loyalty | Consume events, enrol eligible customers, assign welcome offer | Enrolled flag, tier, welcome offer, bonus points |
| 5 | Warehouse | SQL extract from all four systems -> raw -> cleansed/vault -> curated fact | One combined row per account opening |
| 6 | Dashboards | Python reads SQL views and draws Plotly charts | Business insight |

---

## 2. Enrichment rules

Fields listed here MUST follow these rules. Any other column without a stated rule: generate a
plausible random value of the correct type and format.

### 2.1 Identity & Fraud
| Field | Rule |
|---|---|
| `kyc_verification.result` | PASS 90%, REFER 7%, FAIL 3% |
| `kyc_verification.verification_source` | DVS 80%, CREDIT_BUREAU 20% |
| `kyc_verification.attempt_no` | 1 normally; customers with REFER have a 40% chance of a second attempt (attempt_no = 2) |
| `watchlist_screening.match_status` | CLEAR 98%, POSSIBLE 1.8%, CONFIRMED 0.2% |
| `onboarding_session.completed_at` | NULL (abandoned) for 12% of sessions; otherwise started_at + 3 to 15 minutes |
| `onboarding_session.ip_risk_flag` | true for 5% of sessions (e.g. overseas / VPN) |
| Device sharing | Most customers have their own device. Plant 4 fraud rings: one device shared by 5 to 8 customers each |
| `risk_assessment.risk_score` | base = random 0 to 25; +30 if the device is shared by 3 or more customers; +20 if latest KYC = REFER; +25 if watchlist = POSSIBLE; +10 if ip_risk_flag; cap at 100 |
| Hard rules | KYC FAIL or watchlist CONFIRMED -> decision DECLINE regardless of score |
| `risk_assessment.risk_band` | LOW < 40, MEDIUM 40 to 70, HIGH > 70 |
| `risk_assessment.decision` | LOW -> APPROVE, MEDIUM -> REVIEW, HIGH -> DECLINE |
| REVIEW outcome (history only) | 80% later APPROVE, 20% DECLINE, resolved 2 to 48 hours after opening |
| `risk_assessment.reason_codes` | Comma-separated list of the signals that added points: SHARED_DEVICE, KYC_REFER, WATCHLIST_POSSIBLE, IP_RISK, KYC_FAIL, WATCHLIST_CONFIRMED |
| `risk_assessment.assessed_at` | account opened_at + 5 to 120 seconds (REVIEW: final decision time as above) |

### 2.2 Core Banking status after decision
| Decision | account_status | activated_at |
|---|---|---|
| APPROVE | ACTIVE | = decision time |
| DECLINE | REJECTED | NULL |
| REVIEW (unresolved, live demo only) | PENDING | NULL |

### 2.3 CRM
| Field | Rule |
|---|---|
| `crm_contact.segment` | From age at opening: STUDENT (<23), YOUNG_PROFESSIONAL (23 to 35), FAMILY (36 to 55), SENIOR (>55) |
| `crm_contact.lifecycle_stage` | REJECTED account -> DECLINED; ACTIVE and opened < 30 days ago -> NEW_ONBOARDING; ACTIVE and >= 30 days -> ESTABLISHED; PENDING -> UNDER_REVIEW |
| Welcome task | `crm_case` with case_type = WELCOME_CALL, created only for ACTIVE accounts, opened_at = activated_at + 0 to 24 h |
| Welcome task status | COMPLETED if created more than 7 days ago (85%), otherwise OPEN |
| Declined accounts | crm_financial_account.account_status = REJECTED; no welcome task |

### 2.4 Loyalty
| Field | Rule |
|---|---|
| Eligibility | Enrol only if account_status = ACTIVE AND marketing_consent = true |
| `loyalty_member.tier` | BRONZE for all new members |
| Welcome offer | EVERYDAY -> 5,000 points; SAVINGS -> 3,000; any other product -> 1,000 |
| `member_offer.redemption_status` | For history: REDEEMED 60% if assigned > 14 days ago, else ASSIGNED |
| Declined / rejected accounts | Never enrolled. If an `AccountRejected` event arrives for an enrolled account, set member_status = UNENROLLED and eligible = false |

### 2.5 Core Banking synthetic distributions
| Field | Distribution |
|---|---|
| channel | MOBILE 70%, WEB 20%, BRANCH 10% |
| residential state | NSW 32%, VIC 26%, QLD 20%, WA 10%, SA 7%, TAS/ACT/NT 5% combined |
| age | 18 to 75, weighted toward 23 to 45 |
| marketing_consent | true 70% |
| products | EVERYDAY (55%), SAVINGS (30%), STUDENT (10%, only if age < 25), PREMIUM (5%) |

---

## 3. Synthetic history vs live demo

- History: about 2,000 accounts over the last 90 days, with a gentle upward trend (about 20 to 30% more
  openings per week by the end) and weekday seasonality (fewer at weekends). This makes the growth chart
  meaningful.
- History is seeded DIRECTLY by SQL into each system, applying exactly the rules in section 2, so all
  systems are consistent. History does not need to go through Kafka.
- Live demo accounts (Mia, Alex) go through the REAL pipeline: outbox -> Debezium -> Kafka -> consumers.
- Call `SELECT setseed(0.42);` before random generation so every run produces the same data.
- Generate in dependency order: core banking -> identity & fraud -> core status update -> CRM -> loyalty.

---

## 4. Pipeline decisions (supersede CLAUDE.md where different)

- Core Banking -> other systems: outbox + Debezium + Kafka (unchanged).
- Consumers for Identity & Fraud, CRM and Loyalty: small Python scripts in the lab `python` container.
  They must be idempotent (processed_event) and use versioned upserts.
- Identity & Fraud, CRM and Loyalty -> warehouse: SQL extract scripts (NOT Debezium), incremental using
  a watermark table (ops.extract_watermark). If sources are schemas in the same database, use
  INSERT ... SELECT across schemas. If they are separate databases, use postgres_fdw.
- Core Banking -> warehouse: SQL extract of customer/account tables plus landed outbox events.
- All extract/transform/load logic is SQL, fully commented, runnable top to bottom.

---

## 5. Combined warehouse record

`fact_account_opening` (one row per account) must include at least:

| From | Columns |
|---|---|
| Core Banking | account_id, open_date_key, product, channel, state, age_band, account_status, opened_at, activated_at |
| Identity & Fraud | kyc_result, kyc_attempts, risk_score, risk_band, risk_decision, reason_codes, minutes_to_decision, shared_device_customers |
| CRM | segment, lifecycle_stage, welcome_task_created, welcome_task_status |
| Loyalty | loyalty_enrolled, tier, welcome_offer_points, offer_redeemed |
| Derived | is_approved, is_declined, opening_count (= 1) |

---

## 6. Dashboards and reports

### Rules
- ALL queries are SQL views in the curated layer, defined in `sql/09_reports_dashboards.sql`,
  fully commented. This file is what we show the teacher.
- Python ONLY runs `SELECT * FROM curated.<view>` and draws the chart. No filtering, grouping,
  joining or calculating in Python (pandas only for holding the result).
- Library: Plotly. Output one HTML file per dashboard to `dashboards/output/`
  (e.g. `d1_account_opening_growth.html`), each with several charts. Re-running the script refreshes them.
- Run with: `docker compose exec python python /workspace/dashboards/build_dashboards.py`
- Keep charts simple: line, bar, stacked bar, KPI numbers, one table.

### Dashboard 1: Account Opening Growth (business; UC1)
| View | Columns | Chart |
|---|---|---|
| vw_d1_kpi_today | accounts_opened_today, approved_today, declined_today, openings_this_month | KPI numbers |
| vw_d1_daily_openings | open_date, openings, approved | Line (90 days) |
| vw_d1_openings_by_product_channel | product, channel, openings | Grouped bar |
| vw_d1_openings_by_segment | segment, openings, pct | Bar |
| vw_d1_openings_by_state | state, openings | Bar |
| vw_d1_latest_openings | opened_at, customer_name, product, channel, segment, risk_decision, loyalty_enrolled (latest 15) | Table |

### Dashboard 2: Onboarding Risk & Compliance (UC2)
| View | Columns | Chart |
|---|---|---|
| vw_d2_decision_mix | risk_decision, channel, accounts | Stacked bar |
| vw_d2_risk_band_weekly | week, risk_band, accounts | Stacked bar over time |
| vw_d2_shared_device_rings | device_id, customers_on_device, declined, approved | Bar (devices with 3+ customers) |
| vw_d2_time_to_decision | week, avg_minutes_to_decision, pct_within_5_min | Line |
| vw_d2_top_reason_codes | reason_code, occurrences | Bar |

### Dashboard 3: Customer Engagement & Consistency (UC3)
| View | Columns | Chart |
|---|---|---|
| vw_d3_loyalty_enrolment_by_segment | segment, approved_accounts, enrolled, enrolment_pct | Bar |
| vw_d3_welcome_offers_monthly | month, offers_issued, points_committed, redeemed_pct | Bar + line |
| vw_d3_welcome_call_coverage | week, approved_accounts, welcome_tasks, completed_pct | Line |
| vw_d3_consistency_checks | check_name, violations | KPI / table; checks: declined but enrolled in loyalty; declined but ACTIVE in CRM; approved but missing in CRM; approved + consented but not enrolled. Expected result: all 0 |

---

## 7. Demo script (about 5 minutes of video)

Provide these files in `demo/`:
- `00_dashboard_before.md`: note of KPI values before the demo (or just refresh the dashboards)
- `01_open_account_mia.sql`: insert customer Mia Nguyen (27, Parramatta NSW, consent true, own new device, KYC PASS) and an EVERYDAY account via MOBILE. Expected: APPROVE, risk ~12, YOUNG_PROFESSIONAL, BRONZE, 5,000 points.
- `02_open_account_alex_fraud.sql`: insert customer Alex using a device already shared by 5 customers, KYC REFER. Expected: risk ~85, DECLINE, account REJECTED, CRM DECLINED, NOT enrolled in loyalty.
- `03_trace_account.sql`: queries that show one account (parameter: account_id) in every system: outbox row, identity result, core status, CRM record + task, loyalty member + offer, warehouse fact row.
- `04_run_warehouse_load.sql` (or a note of the ordered scripts to run): extract -> transform -> load.

Scenes:
| # | Scene | Show |
|---|---|---|
| 0 | Before | Dashboard 1 KPI: accounts opened today |
| 1 | Mia opens an account | Run 01_open_account_mia.sql |
| 2 | Follow the event | 03_trace_account.sql for Mia: outbox -> Kafka topic -> identity APPROVE -> CRM YOUNG_PROFESSIONAL + welcome task -> loyalty BRONZE + 5,000 points |
| 3 | Warehouse | Run the load scripts; show Mia's row in fact_account_opening |
| 4 | After | Re-run build_dashboards.py: today's openings +1, Mia in latest openings |
| 5 | Fraudster | Run 02_open_account_alex_fraud.sql, trace, reload, refresh: declines +1, shared-device ring grows, consistency checks still 0 |
| 6 | Optional resilience | Stop the loyalty consumer, open an account, show the others still work, restart, loyalty catches up |