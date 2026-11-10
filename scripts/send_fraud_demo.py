"""Send genuinely high-scoring feature rows to exercise the high-fraud alert."""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from fraud_pipeline.scoring import Scorer
from fraud_pipeline.storage import connect, sample_frame
from fraud_pipeline.stream import BROKERS, send


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=100)
    args = parser.parse_args()
    scorer = Scorer()
    with connect() as c:
        records = sample_frame(c, 2000, "fraud-alert-demo").to_dict("records")
    chosen = None
    for record in records:
        record.pop("target", None)
        record.pop("transaction_id", None)
        record["transaction_time"] = datetime.now(timezone.utc).isoformat()
        if scorer.score({"transaction_id": "probe", "data": record})["fraud_flag"]:
            chosen = record
            break
    if chosen is None:
        raise RuntimeError("No high-scoring row found. Publish a trained model through retrain_fraud first.")
    chosen = {k: (v.item() if hasattr(v, "item") else v) for k, v in chosen.items()}
    # Validate serialization before starting the demo stream.
    json.dumps(chosen, allow_nan=False)
    producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True})
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        ident = "alert-demo-" + str(uuid.uuid4())
        chosen["transaction_time"] = datetime.now(timezone.utc).isoformat()
        send(producer, "transactions", {"transaction_id": ident, "data": chosen}, ident)
        time.sleep(.1)


if __name__ == "__main__":
    main()
