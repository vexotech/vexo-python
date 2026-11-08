# vexo-python

Send events and identifies to [Vexo](https://vexo.co) from your Python backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct id.

- Zero runtime dependencies (Python 3.8 or later; `urllib` and `threading` from the standard library).
- Batches messages on a background thread, flushes on size and on an interval, and flushes at interpreter exit within a deadline.
- Retries network errors, `429`, and `5xx` responses (and the messages the API reports as `failed`) with exponential backoff, and honors `Retry-After`.
- Keeps each request under the API's 5 MB body limit, and is safe to create before a fork (gunicorn `--preload`, Celery prefork).

## Install

```bash
pip install vexo-python
```

## Usage

```python
import os
from vexo import Vexo

vexo = Vexo(os.environ["VEXO_API_KEY"])

# Attach traits to the person. Use the same id you pass to identifyDevice() in the app SDK.
vexo.identify("user@example.com", {"plan": "pro", "company": "Acme"})

# Record an event.
vexo.capture("user@example.com", "purchase_completed", {"amount": 49.9, "currency": "USD"})

# Optional: send everything still queued now. It also runs at interpreter exit.
vexo.shutdown()
```

`capture()` and `identify()` only queue the message, so they never block a request. The client registers `shutdown()` with `atexit`; if your process is killed without a normal exit (for example, `SIGKILL`), queued messages are lost. `shutdown()` gives up after `shutdown_timeout` seconds: it aborts the request in flight and any retry wait, and reports what wasn't sent to `on_error`.

Copy your app's API key from the app's [settings](https://docs.vexo.co/settings/app-settings/app-general-information) in the Vexo dashboard. It's the same key your app SDK uses, so it isn't a secret; see [Security](https://docs.vexo.co/server-side/capture#security).

## API

### `Vexo(api_key, **options)`

| Option | Default | Description |
| --- | --- | --- |
| `host` | `https://api.vexo.co` | API base URL. |
| `flush_at` | `100` | Send a batch when this many messages are queued (1 to 500). |
| `flush_interval` | `10.0` | Send whatever is queued every this many seconds. |
| `max_retries` | `3` | Retries for network errors, `429`, and `5xx` responses. |
| `max_queue_size` | `10000` | Messages kept in memory; new messages are dropped once it's full. |
| `timeout` | `10.0` | Request timeout in seconds. |
| `shutdown_timeout` | `10.0` | How long `shutdown()` keeps sending, in seconds. |
| `on_error` | log to the `vexo` logger | Called with send failures and with messages the server dropped. |

### `vexo.capture(distinct_id, event, properties=None, timestamp=None, message_id=None)`

Queues an event. `timestamp` is a `datetime` (a naive one is read as UTC), an ISO 8601 string with an offset, or epoch milliseconds, and defaults to the time you call `capture()`. A `datetime` or a number is sent as integer epoch milliseconds. `message_id` defaults to a random UUID; pass your own, such as an order ID, so that Vexo stores the event once even when your code sends it twice.

### `vexo.identify(distinct_id, traits=None)`

Queues an identify. Traits merge into the person's existing traits; a trait set to `None` is removed.

### `vexo.flush()`

Sends everything queued, blocking until the queue is empty.

### `vexo.shutdown()`

Stops the background thread and sends everything queued, for up to `shutdown_timeout` seconds.

## Errors and dropped messages

The client never raises from `capture()` or `identify()`. Failures go to `on_error`:

- Values that JSON can't encode, such as `Decimal`, `UUID`, or `datetime` properties, are sent as strings. A message that still can't be serialized (for example, a circular reference, or a `NaN` or infinite float, which the API rejects) or that is over 5 MB is dropped when you queue it; the rest of the queue is unaffected.
- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives as an exception with `batch` (the messages it held); a `VexoError` also carries `status`. After that, the next batches get a single attempt until a request succeeds, so an unreachable API doesn't hold up your process.
- A message the API dropped (for example, properties over 64 KB) arrives as a `VexoError` with `dropped`, a list of `{"message", "reason"}`. Messages the API reports as `failed` are retried, and reported the same way if they still fail.
- Messages still queued at the shutdown deadline arrive as one `VexoError` with `batch`.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

```bash
python3 -m unittest
```

## License

MIT
