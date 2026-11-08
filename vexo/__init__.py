"""Send server-side events and identifies to Vexo. Standard library only."""

import atexit
import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime, timezone

__all__ = ["Vexo", "VexoError"]
__version__ = "0.1.0"

DEFAULT_HOST = "https://api.vexo.co"
# The server rejects a batch of more than 500 messages or a body over 5 MB.
MAX_BATCH_SIZE = 500
MAX_BODY_BYTES = 5 * 1024 * 1024
BATCH_ENVELOPE_BYTES = len('{"batch":[]}')
MAX_BACKOFF_SECONDS = 30.0

log = logging.getLogger("vexo")


class VexoError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class Vexo:
    """A client that queues messages and sends them in batches from a background thread."""

    def __init__(self, api_key, host=DEFAULT_HOST, flush_at=100, flush_interval=10.0, max_retries=3,
                 max_queue_size=10000, timeout=10.0, shutdown_timeout=10.0, on_error=None):
        if not api_key:
            raise ValueError("vexo: an API key is required")
        self.api_key = api_key
        self.host = host.rstrip("/")
        self.flush_at = max(1, min(flush_at, MAX_BATCH_SIZE))
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        self.max_queue_size = max_queue_size
        self.timeout = timeout
        self.shutdown_timeout = shutdown_timeout
        self.on_error = on_error or (lambda error: log.error("%s", error))

        self._closed = False
        # Set by a batch that used up its retries: later batches get one attempt until a request succeeds.
        self._circuit_open = False
        self._deadline = None
        self._reset_state()
        atexit.register(self.shutdown)
        if hasattr(os, "register_at_fork"):
            # A forked worker (gunicorn --preload, Celery prefork) gets its own thread and locks; the parent keeps
            # sending what it queued before the fork.
            os.register_at_fork(after_in_child=self._reset_state)

    def capture(self, distinct_id, event, properties=None, timestamp=None, message_id=None):
        """Queue an event. `timestamp` (datetime, ISO 8601 string or epoch ms) defaults to now.

        `message_id` defaults to a random UUID; pass your own (an order ID, say) to dedupe across your own retries.
        """
        if not distinct_id or not event:
            self.on_error(VexoError("vexo: capture needs a distinct_id and an event"))
            return
        self._enqueue({
            "type": "capture",
            "distinctId": str(distinct_id),
            "event": str(event),
            "properties": properties or {},
            "timestamp": _timestamp(timestamp),
        }, message_id)

    def identify(self, distinct_id, traits=None):
        """Queue an identify: merges `traits` into the person with this distinct id."""
        if not distinct_id:
            self.on_error(VexoError("vexo: identify needs a distinct_id"))
            return
        self._enqueue({"type": "identify", "distinctId": str(distinct_id), "traits": traits or {}})

    def flush(self):
        """Send everything queued, blocking until the queue is empty. Send failures go to on_error."""
        with self._send_lock:
            self._drain()

    def shutdown(self):
        """Stop the background thread and send everything queued, giving up after shutdown_timeout seconds.

        What is still queued at the deadline is dropped and reported to on_error. Runs at interpreter exit too.
        """
        if self._deadline is None:
            self._deadline = time.monotonic() + self.shutdown_timeout
        self._closed = True
        self._wake.set()
        if self._worker.is_alive() and threading.current_thread() is not self._worker:
            self._worker.join(self._remaining())
        if not self._send_lock.acquire(timeout=self._remaining()):
            self._drop_queue()
            return
        try:
            self._drain()
        finally:
            self._send_lock.release()

    def _reset_state(self):
        self._queue = []
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._wake = threading.Event()
        self._worker = threading.Thread(target=self._run, name="vexo-flush", daemon=True)
        if not self._closed:
            self._worker.start()

    def _enqueue(self, message, message_id=None):
        if self._closed:
            self.on_error(VexoError("vexo: the client is shut down; message dropped"))
            return
        message["messageId"] = str(message_id or uuid.uuid4())
        try:
            # default=str sends Decimal, UUID, date and similar values as strings.
            body = json.dumps(message, default=str, separators=(",", ":"))
        except (TypeError, ValueError) as error:
            self.on_error(VexoError("vexo: message dropped, it can't be sent as JSON: %s" % error))
            return
        size = len(body.encode("utf-8"))
        if size + BATCH_ENVELOPE_BYTES > MAX_BODY_BYTES:
            self.on_error(VexoError("vexo: message dropped, it is larger than 5 MB"))
            return
        with self._lock:
            full = len(self._queue) >= self.max_queue_size
            if not full:
                self._queue.append((message, body, size))
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

    def _drain(self):
        while True:
            if self._remaining() <= 0:
                self._drop_queue()
                return
            batch = self._next_batch()
            if not batch:
                return
            try:
                self._send(batch)
            except Exception as error:  # noqa: BLE001 -- every send failure is reported, never raised
                error.batch = [message for message, _, _ in batch]
                self.on_error(error)

    # Up to flush_at messages that fit in one request body.
    def _next_batch(self):
        with self._lock:
            size, count = BATCH_ENVELOPE_BYTES, 0
            while count < len(self._queue) and count < self.flush_at \
                    and size + self._queue[count][2] + count <= MAX_BODY_BYTES:
                size += self._queue[count][2]
                count += 1
            batch = self._queue[:count]
            del self._queue[:count]
        return batch

    def _drop_queue(self):
        with self._lock:
            dropped = self._queue[:]
            del self._queue[:]
        if dropped:
            error = VexoError("vexo: shutdown deadline reached; %d message(s) dropped" % len(dropped))
            error.batch = [message for message, _, _ in dropped]
            self.on_error(error)

    # POST /v1/batch. Network errors, 429 and 5xx are retried with exponential backoff (or the server's Retry-After);
    # messages the server reports as failed are sent again the same way. Dropped messages are reported, not retried.
    def _send(self, batch):
        max_retries = 0 if self._circuit_open else self.max_retries
        attempt = 0
        while True:
            try:
                result = self._post(batch)
            except urllib.error.HTTPError as error:
                with error:
                    detail = error.read().decode("utf-8", "replace")
                retryable = error.code == 429 or error.code >= 500
                if not retryable or attempt >= max_retries or self._remaining() <= 0:
                    if retryable:
                        self._circuit_open = True
                    raise VexoError("vexo: batch rejected with HTTP %d: %s" % (error.code, detail), status=error.code) from None
                wait = _retry_after(error)
                self._sleep(_backoff(attempt) if wait is None else wait)
            except (urllib.error.URLError, OSError):
                if attempt >= max_retries or self._remaining() <= 0:
                    self._circuit_open = True
                    raise
                self._sleep(_backoff(attempt))
            else:
                self._circuit_open = False
                self._report(batch, result.get("dropped"), "the server dropped")
                failed = [batch[f["index"]] for f in result.get("failed") or [] if f["index"] < len(batch)]
                if not failed:
                    return
                if attempt >= max_retries or self._remaining() <= 0:
                    self._report(batch, result.get("failed"), "the server could not store")
                    return
                batch = failed
                self._sleep(_backoff(attempt))
            attempt += 1

    def _post(self, batch):
        body = '{"batch":[' + ",".join(json_body for _, json_body, _ in batch) + "]}"
        request = urllib.request.Request(self.host + "/v1/batch", data=body.encode("utf-8"), method="POST", headers={
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
            "User-Agent": "vexo-python/" + __version__,
        })
        timeout = max(0.001, min(self.timeout, self._remaining()))
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return _response_data(response.read())

    def _report(self, batch, results, what):
        if results:
            error = VexoError("vexo: %s %d message(s): %s" % (what, len(results), results[0]["reason"]))
            error.dropped = [{"message": batch[r["index"]][0] if r["index"] < len(batch) else None, "reason": r["reason"]}
                             for r in results]
            self.on_error(error)

    def _remaining(self):
        return float("inf") if self._deadline is None else max(0.0, self._deadline - time.monotonic())

    def _sleep(self, seconds):
        time.sleep(min(seconds, self._remaining()))


def _timestamp(timestamp):
    """Epoch milliseconds as an int; an ISO 8601 string is passed through."""
    if timestamp is None:
        return int(time.time() * 1000)
    if isinstance(timestamp, datetime):
        # A naive datetime is taken as UTC.
        aware = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        return int(aware.timestamp() * 1000)
    if isinstance(timestamp, date):
        return int(datetime(timestamp.year, timestamp.month, timestamp.day, tzinfo=timezone.utc).timestamp() * 1000)
    if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool):
        return int(timestamp)
    return timestamp


def _response_data(body):
    """The envelope's data. A 2xx whose body isn't the envelope still means the API accepted the batch (SPEC W6)."""
    try:
        data = json.loads(body.decode("utf-8")).get("data")
    except (ValueError, AttributeError):
        return {}
    return data if isinstance(data, dict) else {}


def _backoff(attempt):
    base = min(2.0 ** attempt, MAX_BACKOFF_SECONDS)
    return base / 2 + random.random() * base / 2


def _retry_after(error):
    try:
        return min(float(error.headers.get("Retry-After")), 60.0)
    except (TypeError, ValueError):
        return None
