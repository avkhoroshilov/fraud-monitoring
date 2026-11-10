"""Metrics count acknowledged scores and committed rows, never attempted work."""
from collections import deque
import time
from prometheus_client import Counter, Gauge, Histogram, REGISTRY

SCORE_BUCKETS = tuple(i / 20 for i in range(21))
LATENCY_BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 15, 30)


class ScoringMetrics:
    def __init__(self, registry=REGISTRY):
        self.transactions = Counter("fraud_scoring_transactions_total", "Acknowledged scored transactions", registry=registry)
        self.duration = Histogram("fraud_scoring_processing_seconds", "Preprocessing, inference and Kafka acknowledgement", buckets=LATENCY_BUCKETS, registry=registry)
        self.scores = Histogram("fraud_scoring_score", "Acknowledged score distribution", buckets=SCORE_BUCKETS, registry=registry)
        self.fraud = Counter("fraud_scoring_fraud_total", "Acknowledged fraud flags", registry=registry)
        self.ratio = Gauge("fraud_scoring_fraud_ratio", "Fraud fraction in the last up to 1000 acknowledged scores", registry=registry)
        self.last_success = Gauge("fraud_scoring_last_success_timestamp_seconds", "Unix time of last acknowledged score, zero before first event", registry=registry)
        self.errors = Counter("fraud_scoring_errors_total", "Scoring failures by bounded reason", ["reason"], registry=registry)
        self.updates = Counter("fraud_model_updates_total", "Successful hot loads, excluding cold start", ["version"], registry=registry)
        self.loaded_at = Gauge("fraud_model_reload_timestamp_seconds", "Actual hot load time; retained for historical annotations", ["version"], registry=registry)
        self.active = Gauge("fraud_model_active", "One for the currently loaded model version", ["version"], registry=registry)
        self.reload_errors = Counter("fraud_model_reload_errors_total", "Failed attempts to replace the active model", registry=registry)
        self.flags = deque(maxlen=1000)
        for reason in ("invalid_input", "delivery"):
            self.errors.labels(reason)

    def model_loaded(self, previous, current):
        if previous:
            self.active.labels(previous).set(0)
            self.updates.labels(current).inc()
            self.loaded_at.labels(current).set(time.time())
        self.active.labels(current).set(1)

    def recorded(self, result, seconds):
        self.transactions.inc()
        self.duration.observe(seconds)
        self.scores.observe(result["score"])
        self.fraud.inc(result["fraud_flag"])
        self.flags.append(result["fraud_flag"])
        self.ratio.set(sum(self.flags) / len(self.flags))
        self.last_success.set(time.time())


class WriterMetrics:
    def __init__(self, registry=REGISTRY):
        self.rows = Counter("fraud_writer_rows_total", "New rows committed to PostgreSQL", registry=registry)
        self.duration = Histogram("fraud_writer_commit_seconds", "SQL and successful commit duration per Kafka batch", buckets=LATENCY_BUCKETS, registry=registry)
        self.fraud = Counter("fraud_writer_fraud_total", "New committed rows flagged as fraud", registry=registry)
        self.scores = Histogram("fraud_writer_score", "New committed score distribution", buckets=SCORE_BUCKETS, registry=registry)
        self.duplicates = Counter("fraud_writer_duplicates_total", "Rows skipped by transaction_id uniqueness", registry=registry)
        self.errors = Counter("fraud_writer_errors_total", "Writer failures by bounded reason", ["reason"], registry=registry)
        for reason in ("invalid_input", "database"):
            self.errors.labels(reason)

    def recorded(self, inserted, attempted, seconds):
        self.duration.observe(seconds)
        self.duplicates.inc(attempted - len(inserted))
        for result in inserted:
            self.rows.inc()
            self.fraud.inc(result["fraud_flag"])
            self.scores.observe(result["score"])
