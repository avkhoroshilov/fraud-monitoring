"""Hot reload uses the same committed registry row as the training pointer."""
import logging
import math
import threading
from datetime import datetime, timezone
import joblib
import pandas as pd
from fraud_pipeline.storage import connect, get_state, digest

logger = logging.getLogger(__name__)


class Scorer:
    def __init__(self, metrics=None):
        self.model = None
        self.version = None
        self.lock = threading.Lock()
        self.metrics = metrics

    def score(self, message):
        if not isinstance(message, dict) or not isinstance(message.get("data"), dict):
            raise ValueError("Expected an object with a data object")
        ident = message.get("transaction_id")
        if not isinstance(ident, str) or not ident.strip() or len(ident) > 200 or "\x00" in ident:
            raise ValueError("Invalid transaction_id")
        with self.lock:
            with connect() as c:
                active = get_state(c, "active")
            if active is None:
                raise RuntimeError("No published model; run data-init")
            if active["version"] != self.version:
                try:
                    if digest(active["path"]) != active["sha256"]:
                        raise ValueError("Model checksum mismatch")
                    new = joblib.load(active["path"])
                    # Assign model and version together only after successful loading.
                    previous = self.version
                    self.model, self.version = new, active["version"]
                    if self.metrics:
                        self.metrics.model_loaded(previous, self.version)
                    logger.info("Loaded model_version=%s", self.version)
                except Exception as exc:
                    if self.metrics:
                        self.metrics.reload_errors.inc()
                    if self.model is None:
                        raise RuntimeError("Cannot load the active model at startup") from exc
                    logger.exception("Reload failed; retaining model_version=%s", self.version)
            probability = float(self.model.predict_proba(pd.DataFrame([message["data"]]))[0,1])
            if not math.isfinite(probability) or not 0 <= probability <= 1:
                raise ValueError("Model returned an invalid probability")
            data = message["data"]
            metadata = {}
            for key in ("us_state", "merch", "cat_id"):
                value = data.get(key)
                text = "" if value is None else str(value).replace("\x00", "").strip()
                metadata[key] = text[:500] or "unknown"
            try:
                amount = float(data.get("amount"))
                metadata["amount"] = amount if math.isfinite(amount) and amount >= 0 else None
            except (TypeError, ValueError):
                metadata["amount"] = None
            metadata["processed_at"] = datetime.now(timezone.utc).isoformat()
            return dict(transaction_id=message["transaction_id"], score=probability,
                        fraud_flag=int(probability >= .5), model_version=self.version, metadata=metadata)
