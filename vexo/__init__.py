"""Send server-side events and identifies to Vexo. Standard library only."""

import atexit
import json
import logging
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

__all__ = ["Vexo"]
__version__ = "0.1.0"

DEFAULT_HOST = "https://api.vexo.co"
# The server rejects a batch of more than 500 messages.
MAX_BATCH_SIZE = 500
MAX_BACKOFF_SECONDS = 30.0

log = logging.getLogger("vexo")


class Vexo:
    """A client that queues messages and sends them in batches from a background thread."""

    def __init__(self, api_key, host=DEFAULT_HOST, flush_at=100, flush_interval=10.0, max_retries=3,
                 max_queue_size=10000, timeout=10.0, on_error=None):
        if not api_key:
            raise ValueError("vexo: an API key is required")
        self.api_key = api_key
        self.host = host.rstrip("/")
        self.flush_at = max(1, min(flush_at, MAX_BATCH_SIZE))
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        self.max_queue_size = max_queue_size
        self.timeout = timeout
        self.on_error = on_error or (lambda error: log.error("%s", error))

        self._queue = []
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._wake = threading.Event()
        self._closed = False
        self._worker = threading.Thread(target=self._run, name="vexo-flush", daemon=True)
        self._worker.start()
        atexit.register(self.shutdown)

    def capture(self, distinct_id, event, properties=None, timestamp=None):
        """Queue an event. `timestamp` (datetime, ISO 8601 string or epoch ms) defaults to now."""
        if not distinct_id or not event:
            self.on_error(ValueError("vexo: capture needs a distinct_id and an event"))
            return
        self._enqueue({
            "type": "capture",
            "distinctId": str(distinct_id),
            "event": str(event),
            "properties": properties or {},
            "timestamp": _iso(timestamp),
        })

    def identify(self, distinct_id, traits=None):
        """Queue an identify: merges `traits` into the person with this distinct id."""
        if not distinct_id:
            self.on_error(ValueError("vexo: identify needs a distinct_id"))
            return
        self._enqueue({"type": "identify", "distinctId": str(distinct_id), "traits": traits or {}})

    def flush(self):
        """Send everything queued, blocking until the queue is empty. Send failures go to on_error."""
        with self._send_lock:
            while True:
                with self._lock:
                    batch = self._queue[:self.flush_at]
                    del self._queue[:self.flush_at]
                if not batch:
                    return
                try:
                    result = self._send(batch)
                    dropped = (result or {}).get("dropped") or []
                    if dropped:
                        reason = dropped[0]["reason"]
                        error = VexoError("vexo: the server dropped %d message(s): %s" % (len(dropped), reason))
                        error.dropped = [{"message": batch[d["index"]], "reason": d["reason"]} for d in dropped]
                        self.on_error(error)
                except Exception as error:  # noqa: BLE001 -- every send failure is reported, never raised
                    error.batch = batch
                    self.on_error(error)

    def shutdown(self):
        """Stop the background thread and send everything queued. Runs at interpreter exit too."""
        self._closed = True
        self._wake.set()
        if self._worker.is_alive() and threading.current_thread() is not self._worker:
            self._worker.join(self.timeout)
        self.flush()

    def _enqueue(self, message):
        if self._closed:
            self.on_error(VexoError("vexo: the client is shut down; message dropped"))
            return
        message["messageId"] = str(uuid.uuid4())
        with self._lock:
            full = len(self._queue) >= self.max_queue_size
            if not full:
                self._queue.append(message)
            ready = len(self._queue) >= self.flush_at
        if full:
            self.on_error(VexoError("vexo: the queue is full; message dropped"))
        elif ready:
            self._wake.set()

    def _run(self):
        while not self._closed:
            self._wake.wait(self.flush_interval)
            self._wake.clear()
            if not self._closed:
                self.flush()

    # POST /v1/batch, retrying network errors, 429 and 5xx with exponential backoff (or the server's Retry-After).
    def _send(self, batch):
        body = json.dumps({"batch": batch}).encode("utf-8")
        request = urllib.request.Request(self.host + "/v1/batch", data=body, method="POST", headers={
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
            "User-Agent": "vexo-python/" + __version__,
        })
        attempt = 0
        while True:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8")).get("data")
            except urllib.error.HTTPError as error:
                with error:
                    detail = error.read().decode("utf-8", "replace")
                retryable = error.code == 429 or error.code >= 500
                if not retryable or attempt >= self.max_retries:
                    raise VexoError("vexo: batch rejected with HTTP %d: %s" % (error.code, detail), status=error.code) from None
                wait = _retry_after(error)
                time.sleep(_backoff(attempt) if wait is None else wait)
            except (urllib.error.URLError, OSError):
                if attempt >= self.max_retries:
                    raise
                time.sleep(_backoff(attempt))
            attempt += 1


class VexoError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


def _iso(timestamp):
    if timestamp is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(timestamp, datetime):
        # A naive datetime is taken as UTC, so the server always gets an offset.
        return (timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)).isoformat()
    return timestamp


def _backoff(attempt):
    base = min(2.0 ** attempt, MAX_BACKOFF_SECONDS)
    return base / 2 + random.random() * base / 2


def _retry_after(error):
    try:
        return min(float(error.headers.get("Retry-After")), 60.0)
    except (TypeError, ValueError):
        return None
