# West City Bank Data Model — Mermaid source

Edit at https://mermaid.live, or import into draw.io via Arrange > Insert > Advanced > Mermaid (one diagram at a time).

Syntax: `type column_name KEY "comment"` inside an entity block; relationships use crow's-foot notation, e.g. `customer ||--o{ account : "holds"` (|| exactly one, o{ zero or many, |{ one or many, o| zero or one).

## 0. System landscape (flowchart)

```mermaid
flowchart LR
  subgraph SRC[Source systems]
    CB[(core_banking<br/>system of record)]
    IF[(identity_fraud)]
    CRM[(crm)]
    LOY[(loyalty)]
  end
  CB -->|outbox rows| DZ1[Debezium<br/>outbox event router]
  DZ1 --> K1{{Kafka<br/>bank.account.events<br/>bank.customer.events}}
  K1 -->|risk scoring<br/>Python consumer| IF
  K1 -->|versioned upsert<br/>Python consumer| CRM
  K1 -->|versioned upsert<br/>Python consumer| LOY
  K1 -->|dw lander| STG
  IF -.->|risk decision<br/>simulated in prototype| CB
  IF & CRM & LOY -->|table CDC| DZ2[Debezium<br/>table connectors]
  DZ2 --> K2{{Kafka<br/>cdc.* topics}}
  K2 -->|dw lander| STG
  subgraph DW[westcity_dw]
    STG[staging] --> RV[raw_vault] --> MART[mart]
    STG --> OPS[ops: audit + recon]
  end
  CB & CRM & LOY -.->|postgres_fdw<br/>recon + nightly batch| OPS
  MART & OPS --> MB[Metabase<br/>business]
  MART & OPS --> GF[Grafana<br/>technical]
```

## 1. core_banking — core banking and customer master

```mermaid
erDiagram
  customer ||--o{ customer_address : "lives at"
  customer ||--o{ account_holder : "holds"
  account ||--|{ account_holder : "owned by"
  product ||--o{ account : "defines"
  branch ||--o{ account : "domiciles"
  customer {
    bigint customer_id PK
    varchar first_name
    varchar last_name
    date date_of_birth
    varchar email
    varchar mobile "E.164 format"
    boolean marketing_consent
    varchar statement_delivery "PAPER or EMAIL"
    date customer_since
    int version "aggregate version"
    timestamptz created_at
    timestamptz updated_at
  }
  customer_address {
    bigint address_id PK
    bigint customer_id FK
    varchar address_type "RESIDENTIAL or POSTAL"
    varchar line1
    varchar suburb
    char state "NSW VIC QLD ..."
    char postcode "4 digits"
    date valid_from
    date valid_to "null if current"
  }
  branch {
    int branch_id PK
    char bsb UK "6-digit BSB"
    varchar branch_name
    char state
  }
  product {
    varchar product_code PK
    varchar product_name
    varchar product_type "TRANSACTION or SAVINGS"
    numeric monthly_fee
    numeric interest_rate
    boolean is_active
  }
  account {
    bigint account_id PK
    int branch_id FK
    char account_number "UK with branch_id"
    varchar product_code FK
    varchar account_status "PENDING ACTIVE REJECTED CLOSED"
    varchar channel "MOBILE BRANCH WEB"
    timestamptz opened_at
    timestamptz activated_at
    timestamptz closed_at
    uuid onboarding_session_id "ref identity_fraud, logical key"
    int version "aggregate version"
  }
  account_holder {
    bigint account_id PK,FK
    bigint customer_id PK,FK
    varchar holder_role "PRIMARY or JOINT"
    timestamptz added_at
  }
  outbox {
    uuid id PK "event id"
    varchar aggregatetype "account or customer"
    varchar aggregateid "account_id or customer_id"
    varchar type "CustomerCreated AccountOpened ..."
    int event_version "aggregate version after change"
    jsonb payload "full business event, see docs/event-contract.md"
    bigint tx_id "txid_current(), for lineage"
    timestamptz created_at
  }
```

## 2. identity_fraud — identity verification and fraud platform

```mermaid
erDiagram
  device ||--o{ onboarding_session : "used in"
  onboarding_session ||--o{ kyc_verification : "produces"
  onboarding_session ||--o{ risk_assessment : "context for"
  device {
    bigint device_id PK
    varchar device_fingerprint UK "hashed"
    varchar os
    timestamptz first_seen_at
  }
  onboarding_session {
    uuid session_id PK
    bigint customer_id "ref core_banking"
    bigint device_id FK
    inet ip_address
    varchar channel
    timestamptz started_at
    timestamptz completed_at
  }
  kyc_verification {
    bigint kyc_id PK
    uuid session_id FK
    bigint customer_id "ref core_banking"
    varchar document_type "PASSPORT DRIVER_LICENCE MEDICARE"
    varchar document_token "hash only, never the raw number"
    varchar verification_source "DVS or CREDIT_BUREAU"
    varchar result "PASS FAIL REFER"
    timestamptz verified_at
  }
  watchlist_screening {
    bigint screening_id PK
    bigint customer_id "ref core_banking"
    varchar list_type "SANCTIONS or PEP"
    varchar match_status "CLEAR POSSIBLE CONFIRMED"
    timestamptz screened_at
  }
  risk_assessment {
    bigint assessment_id PK
    bigint account_id "ref core_banking"
    bigint customer_id "ref core_banking"
    uuid session_id FK
    uuid source_event_id "AccountOpened event id"
    numeric risk_score "0 to 100"
    varchar decision "APPROVE REVIEW DECLINE"
    text reason_codes
    varchar model_version
    timestamptz assessed_at
  }
  processed_event {
    uuid event_id PK
    timestamptz processed_at
  }
```

## 3. crm — customer relationship management

```mermaid
erDiagram
  crm_contact ||--o{ crm_financial_account : "owns"
  crm_contact ||--o{ crm_case : "raises"
  crm_contact {
    bigint contact_id PK
    bigint customer_id UK "golden key"
    varchar full_name
    varchar email
    varchar mobile
    boolean marketing_consent
    int source_version
    timestamptz synced_at
  }
  crm_financial_account {
    bigint fin_account_id PK
    bigint account_id UK "golden key"
    bigint contact_id FK
    varchar product_code
    varchar account_status
    timestamptz opened_at
    int source_version
    timestamptz synced_at
  }
  crm_case {
    bigint case_id PK
    bigint contact_id FK
    varchar case_type "ONBOARDING_HELP COMPLAINT ENQUIRY"
    varchar case_status
    varchar owner_user
    timestamptz opened_at
    timestamptz closed_at
  }
  processed_event {
    uuid event_id PK
    timestamptz processed_at
  }
```

## 4. loyalty — rewards and loyalty platform

```mermaid
erDiagram
  loyalty_member ||--o{ loyalty_account_link : "earns through"
  loyalty_member ||--o{ member_offer : "receives"
  loyalty_offer ||--o{ member_offer : "assigned as"
  loyalty_member {
    bigint member_id PK
    bigint customer_id UK "golden key"
    boolean marketing_consent
    varchar member_status "ENROLLED INELIGIBLE UNENROLLED"
    timestamptz joined_at
    int source_version
    timestamptz synced_at
  }
  loyalty_account_link {
    bigint account_id PK "golden key"
    bigint member_id FK
    varchar product_code
    varchar account_status
    boolean eligible
    int source_version
    timestamptz synced_at
  }
  loyalty_offer {
    int offer_id PK
    varchar offer_name
    varchar eligible_product_code
    int bonus_points
    date valid_from
    date valid_to
  }
  member_offer {
    bigint member_id PK,FK
    int offer_id PK,FK
    timestamptz assigned_at
    varchar redemption_status
  }
  processed_event {
    uuid event_id PK
    timestamptz processed_at
  }
```

## 5. westcity_dw.raw_vault — Data Vault 2.0

```mermaid
erDiagram
  hub_customer ||--o{ link_account_holder : ""
  hub_account ||--o{ link_account_holder : ""
  hub_account ||--o{ link_account_product : ""
  hub_product ||--o{ link_account_product : ""
  hub_customer ||--o{ link_customer_device : ""
  hub_device ||--o{ link_customer_device : ""
  hub_customer ||--o{ sat_customer_details : "history"
  hub_customer ||--o{ sat_customer_kyc : "history"
  hub_account ||--o{ sat_account_state : "history"
  hub_account ||--o{ sat_account_risk : "history"
  hub_customer {
    char hk_customer PK "MD5 of customer_id"
    bigint customer_id "business key"
    timestamptz load_dts
    varchar record_source
  }
  hub_account {
    char hk_account PK
    bigint account_id "business key"
    timestamptz load_dts
    varchar record_source
  }
  hub_product {
    char hk_product PK
    varchar product_code "business key"
    timestamptz load_dts
    varchar record_source
  }
  hub_device {
    char hk_device PK
    varchar device_fingerprint "business key"
    timestamptz load_dts
    varchar record_source
  }
  link_account_holder {
    char hk_account_holder PK
    char hk_account FK
    char hk_customer FK
    varchar holder_role
    timestamptz load_dts
    varchar record_source
  }
  link_account_product {
    char hk_account_product PK
    char hk_account FK
    char hk_product FK
    timestamptz load_dts
    varchar record_source
  }
  link_customer_device {
    char hk_customer_device PK
    char hk_customer FK
    char hk_device FK
    timestamptz load_dts
    varchar record_source
  }
  sat_customer_details {
    char hk_customer PK,FK
    timestamptz load_dts PK
    char hash_diff
    varchar first_name
    varchar last_name
    date date_of_birth
    varchar email
    varchar mobile
    char residential_state
    boolean marketing_consent
    timestamptz src_commit_ts
    varchar record_source
    bigint run_id
  }
  sat_customer_kyc {
    char hk_customer PK,FK
    timestamptz load_dts PK
    char hash_diff
    varchar kyc_result
    varchar document_type
    varchar watchlist_status
    timestamptz verified_at
    varchar record_source
    bigint run_id
  }
  sat_account_state {
    char hk_account PK,FK
    timestamptz load_dts PK
    char hash_diff
    varchar account_status
    varchar channel
    char bsb
    char account_number
    timestamptz opened_at
    timestamptz activated_at
    int source_version
    timestamptz src_commit_ts
    varchar record_source
    bigint run_id
  }
  sat_account_risk {
    char hk_account PK,FK
    timestamptz load_dts PK
    char hash_diff
    numeric risk_score
    varchar decision
    varchar model_version
    timestamptz assessed_at
    varchar record_source
    bigint run_id
  }
```

## 6. westcity_dw.mart — star schema

```mermaid
erDiagram
  dim_date ||--o{ fact_account_opening : "opened on"
  dim_customer ||--o{ fact_account_opening : ""
  dim_product ||--o{ fact_account_opening : ""
  dim_channel ||--o{ fact_account_opening : ""
  dim_branch ||--o{ fact_account_opening : ""
  dim_date ||--o{ fact_event_propagation : ""
  dim_date {
    int date_key PK "YYYYMMDD"
    date calendar_date
    int day_of_week
    int month
    int quarter
    int year
    boolean is_business_day
  }
  dim_customer {
    bigint customer_key PK "surrogate"
    bigint customer_id
    varchar age_band
    char residential_state
    boolean marketing_consent
    timestamptz valid_from "SCD type 2"
    timestamptz valid_to "null if current"
    boolean is_current
  }
  dim_product {
    int product_key PK
    varchar product_code
    varchar product_name
    varchar product_type
  }
  dim_channel {
    int channel_key PK
    varchar channel
  }
  dim_branch {
    int branch_key PK
    char bsb
    varchar branch_name
    char state
  }
  fact_account_opening {
    bigint account_id PK "degenerate dimension"
    int open_date_key FK
    bigint customer_key FK "PRIMARY holder"
    int product_key FK
    int channel_key FK
    int branch_key FK
    varchar account_status
    varchar kyc_result
    varchar risk_decision
    numeric risk_score
    numeric minutes_to_activation
    int shared_device_customers
    int opening_count "always 1"
  }
  fact_event_propagation {
    uuid event_id PK
    varchar target_system PK "FRAUD CRM LOYALTY DW"
    bigint account_id
    int date_key FK
    varchar event_type
    timestamptz event_created_ts
    timestamptz applied_ts
    numeric latency_seconds
    boolean within_sla
  }
```

## 7. westcity_dw.ops — audit, data quality and reconciliation

```mermaid
erDiagram
  etl_run ||--o{ dq_quarantine : "rejected in"
  recon_run ||--o{ recon_break : "finds"
  recon_sla ||--o{ recon_break : "classifies"
  etl_run {
    bigint run_id PK
    varchar job_name
    varchar script_version "git commit hash"
    timestamptz started_at
    timestamptz finished_at
    varchar status
    int rows_read
    int rows_loaded
    int rows_rejected
  }
  dq_quarantine {
    bigint quarantine_id PK
    bigint run_id FK
    varchar source_table
    varchar record_key
    varchar rule_name
    text reason
    jsonb raw_record
    timestamptz quarantined_at
  }
  recon_sla {
    varchar target_system PK "CRM LOYALTY FRAUD DW"
    int sla_minutes
  }
  recon_run {
    bigint recon_run_id PK
    timestamptz started_at
    timestamptz finished_at
    int accounts_checked
    int breaks_found
  }
  recon_break {
    bigint break_id PK
    bigint recon_run_id FK
    varchar target_system FK
    bigint account_id
    varchar break_type "MISSING STALE MISMATCH"
    varchar severity "CRITICAL HIGH LOW"
    timestamptz first_detected_at
    varchar status "IN_FLIGHT OPEN AUTO_REPAIRED RESOLVED"
    timestamptz repair_requested_at "set by manual repair, e.g. snapshot"
    timestamptz resolved_at
    text note
  }
  consumer_health {
    varchar consumer_name PK "FRAUD CRM LOYALTY DW_LANDER"
    varchar topic PK
    int kafka_partition PK
    bigint committed_offset
    bigint end_offset
    bigint lag
    timestamptz updated_at
  }
```
