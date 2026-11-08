# vexo-python

Send events and identifies to [Vexo](https://vexo.co) from your Python backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct id.

- Zero runtime dependencies (Python 3.8 or later; `urllib` and `threading` from the standard library).
- Batches messages on a background thread, flushes on size and on an interval, and flushes at interpreter exit.
- Retries network errors, `429`, and `5xx` responses with exponential backoff, and honors `Retry-After`.

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

`capture()` and `identify()` only queue the message, so they never block a request. The client registers `shutdown()` with `atexit`; if your process is killed without a normal exit (for example, `SIGKILL`), queued messages are lost.

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
| `on_error` | log to the `vexo` logger | Called with send failures and with messages the server dropped. |

### `vexo.capture(distinct_id, event, properties=None, timestamp=None)`

Queues an event. `timestamp` is a `datetime` (a naive one is read as UTC), an ISO 8601 string, or epoch milliseconds, and defaults to the time you call `capture()`.

### `vexo.identify(distinct_id, traits=None)`

Queues an identify. Traits merge into the person's existing traits; a trait set to `None` is removed.

### `vexo.flush()`

Sends everything queued, blocking until the queue is empty.

### `vexo.shutdown()`

Stops the background thread and sends everything queued.

## Errors and dropped messages

The client never raises from `capture()` or `identify()`. Failures go to `on_error`:

- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives as an exception with `batch` (the messages it held); a `VexoError` also carries `status`.
- A message the API accepted the request for but dropped (for example, properties over 64 KB) arrives as a `VexoError` with `dropped`, a list of `{"message", "reason"}`.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

```bash
python3 -m unittest
```

## License

MIT
