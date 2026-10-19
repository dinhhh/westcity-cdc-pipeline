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
