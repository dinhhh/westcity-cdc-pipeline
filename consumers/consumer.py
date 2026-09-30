"""
consumers/consumer.py - Kafka consumer for Identity & Fraud, CRM, Loyalty
or the data warehouse.

Docker Compose runs this ONE script four times, once per downstream system:
    python -u /workspace/consumers/consumer.py fraud     (service consumer-fraud)
    python -u /workspace/consumers/consumer.py crm       (service consumer-crm)
    python -u /workspace/consumers/consumer.py loyalty   (service consumer-loyalty)
    python -u /workspace/consumers/consumer.py dw        (service consumer-dw)

What it does, in a loop:
  1. Read the next event from Kafka (written there by Debezium from
     core_banking.outbox). fraud / crm / loyalty read bank.account.events;
     dw reads bank.account.events and bank.customer.events.
  2. If this system needs that event type, call the system's SQL function
     in ONE database transaction:
        fraud   -> fraud_assess_account    (sql/05_fraud_scoring.sql)
        crm     -> crm_onboard_account     (sql/04_crm_onboarding.sql)
        loyalty -> loyalty_onboard_account (sql/06_loyalty_onboarding.sql)
        dw      -> bronze.land_core_event  (sql/08_warehouse_setup.sql)
     All business rules live in those SQL functions; Python only moves data.
  3. After the database COMMIT, commit the Kafka offset: this tells Kafka
     "this consumer group has finished this event".

Why this order is safe:
  * If the script stops after step 2 but before step 3, Kafka sends the same
    event again after a restart (at-least-once delivery). Each SQL function
    records the event_id (in processed_event, or as the key of
    bronze.core_event) in the same transaction, so the repeat is skipped
    (idempotent consumer).
  * If the database fails in step 2, the transaction rolls back and the
    script stops WITHOUT committing the offset; Docker restarts it and the
    event is read again. No event is ever lost.
  * Each system has its own consumer group, so each keeps its own position
    in the topic and gets every event, whatever the others are doing.
"""
import json
import signal
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError

ACCOUNT_TOPIC = "bank.account.events"
CUSTOMER_TOPIC = "bank.customer.events"


def primary_holder(account):
    """The PRIMARY holder of the account, including a copy of the customer."""
    return next(h for h in account["holders"] if h["holder_role"] == "PRIMARY")


# -----------------------------------------------------------------------------
# One function per system: take the event (and the Kafka message it came in),
# call the system's SQL function.
# The event payload is built in sql/03_outbox_triggers.sql:
#   event["event_id"], event["event_type"],
#   event["data"] = the account (account_id, version, product_code,
#                   account_status, opened_at, activated_at,
#                   onboarding_session_id, holders[ {customer_id,
#                   holder_role, customer: {version, date_of_birth,
#                   marketing_consent, ...}} ])
# Fields that are NULL in the database (e.g. activated_at of a PENDING
# account) are LEFT OUT of the event by Debezium, so optional fields are read
# with .get(), which gives None (= SQL NULL) when the key is missing.
# -----------------------------------------------------------------------------

def fraud(cur, event, msg):
    """Identity & Fraud: score the new account and decide."""
    account = event["data"]
    holder = primary_holder(account)
    cur.execute(
        "SELECT fraud_assess_account(%s, %s, %s, %s, %s)",
        (account["account_id"],
         holder["customer_id"],
         account.get("onboarding_session_id"),
         account["opened_at"],
         event["event_id"]))


def crm(cur, event, msg):
    """CRM: contact + segment, account + lifecycle stage, welcome-call task."""
    account = event["data"]
    holder = primary_holder(account)
    customer = holder["customer"]
    cur.execute(
        "SELECT crm_onboard_account(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (holder["customer_id"],
         customer["date_of_birth"],
         customer["version"],
         account["account_id"],
         account["account_status"],
         account["opened_at"],
         account.get("activated_at"),
         account["version"],
         event["event_id"]))


def loyalty(cur, event, msg):
    """Loyalty: enrol if ACTIVE + consent and give the welcome offer;
    unenrol if the account is rejected."""
    account = event["data"]
    holder = primary_holder(account)
    customer = holder["customer"]
    cur.execute(
        "SELECT loyalty_onboard_account(%s, %s, %s, %s, %s, %s, %s, %s)",
        (holder["customer_id"],
         account["account_id"],
         account["product_code"],
         account["account_status"],
         customer["marketing_consent"],
         account.get("activated_at"),
         account["version"],
         event["event_id"]))


def dw(cur, event, msg):
    """Data warehouse: land the whole event, unchanged, in bronze, with
    where it was read in Kafka (lineage)."""
    cur.execute(
        "SELECT bronze.land_core_event(%s, %s, %s, %s)",
        (json.dumps(event),
         msg.topic(),
         msg.partition(),
         msg.offset()))


ALL_EVENTS = {"CustomerCreated", "CustomerDetailsUpdated",
              "AccountOpened", "AccountActivated", "AccountRejected"}

# system name -> (database, topics, event types it needs, function above)
SYSTEMS = {
    "fraud":   ("identity_fraud", [ACCOUNT_TOPIC], {"AccountOpened"}, fraud),
    "crm":     ("crm", [ACCOUNT_TOPIC], {"AccountOpened", "AccountActivated", "AccountRejected"}, crm),
    "loyalty": ("loyalty", [ACCOUNT_TOPIC], {"AccountActivated", "AccountRejected"}, loyalty),
    "dw":      ("westcity_dw", [ACCOUNT_TOPIC, CUSTOMER_TOPIC], ALL_EVENTS, dw),
}


def main():
    name = sys.argv[1]                                   # fraud / crm / loyalty / dw
    database, topics, needed_events, handle = SYSTEMS[name]

    # `docker compose stop` sends SIGTERM: exit through the finally block
    # below, so the consumer leaves its group cleanly and a restart gets its
    # partitions back straight away.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    db = psycopg2.connect(host="postgres", port=5432, dbname=database,
                          user="westcity_admin", password="lab_admin")

    consumer = Consumer({
        "bootstrap.servers": "kafka:29092",
        "group.id": f"{name}-consumer",      # Kafka stores this group's position
        "auto.offset.reset": "earliest",     # first start: begin at the oldest event
        "enable.auto.commit": False,         # we commit ourselves, after the DB commit
        # A topic only appears when its first event is published;
        # check for it every 5 seconds (the default is every 5 minutes).
        "topic.metadata.refresh.interval.ms": 5000,
    })
    consumer.subscribe(topics)
    print(f"[{name}] writing to database {database}; waiting for events on {', '.join(topics)}")

    try:
        while True:
            # 1. Read the next event (wait up to 1 second).
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue                 # topic not created yet: keep waiting
                raise Exception(msg.error())

            event = json.loads(msg.value())
            where = (f"{event['aggregate_type']} {msg.key().decode()}, "
                     f"{msg.topic()} partition {msg.partition()}, offset {msg.offset()}")

            # 2. Apply it in one database transaction: `with db` COMMITs at
            #    the end, or ROLLs BACK and re-raises the error.
            if event["event_type"] in needed_events:
                with db:
                    with db.cursor() as cur:
                        handle(cur, event, msg)
                        result = cur.fetchone()[0]
                print(f"[{name}] {event['event_type']} ({where}) -> {result}")
            else:
                print(f"[{name}] {event['event_type']} ({where}) -> not needed here")

            # 3. Only now tell Kafka the event is done.
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
