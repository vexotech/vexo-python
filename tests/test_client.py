import json
import threading
import time
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from vexo import Vexo


class FakeApi:
    """A local API that records each batch request and answers with the next scripted response."""

    def __init__(self, responses=None):
        self.requests = []
        self.responses = list(responses or [])
        api = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                api.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                status, headers, payload = api.responses.pop(0) if api.responses else (200, {}, None)
                if payload is None:
                    payload = {"success": True, "code": 200, "data": {"accepted": len(body["batch"]), "dropped": []}}
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.host = "http://127.0.0.1:%d" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class VexoTest(unittest.TestCase):
    def client(self, responses=None, **options):
        self.api = FakeApi(responses)
        self.addCleanup(self.api.close)
        self.errors = []
        options.setdefault("flush_interval", 60)
        vexo = Vexo("test-key", host=self.api.host, on_error=self.errors.append, **options)
        self.addCleanup(vexo.shutdown)
        return vexo

    def test_sends_queued_messages_as_one_batch_with_the_api_key(self):
        vexo = self.client()
        vexo.identify("user_1", {"plan": "pro"})
        vexo.capture("user_1", "purchase", {"amount": 42}, timestamp=datetime(2026, 11, 7, 12, 0, 0))
        vexo.shutdown()

        self.assertEqual(len(self.api.requests), 1)
        request = self.api.requests[0]
        self.assertEqual(request["path"], "/v1/batch")
        self.assertEqual(request["headers"]["X-Api-Key"], "test-key")
        self.assertTrue(request["headers"]["User-Agent"].startswith("vexo-python/"))
        identify, capture = request["body"]["batch"]
        self.assertEqual((identify["type"], identify["distinctId"], identify["traits"]), ("identify", "user_1", {"plan": "pro"}))
        self.assertEqual((capture["type"], capture["event"], capture["properties"]), ("capture", "purchase", {"amount": 42}))
        self.assertEqual(capture["timestamp"], "2026-11-07T12:00:00+00:00")
        self.assertNotEqual(identify["messageId"], capture["messageId"])
        self.assertEqual(self.errors, [])

    def test_defaults_the_timestamp_to_now_with_an_offset(self):
        vexo = self.client()
        vexo.capture("u", "a")
        vexo.flush()
        timestamp = self.api.requests[0]["body"]["batch"][0]["timestamp"]
        self.assertIsNotNone(datetime.fromisoformat(timestamp).tzinfo)

    def test_flushes_when_the_queue_reaches_flush_at(self):
        vexo = self.client(flush_at=2)
        vexo.capture("u", "a")
        vexo.capture("u", "b")
        self.wait_for_requests(1)
        self.assertEqual(len(self.api.requests[0]["body"]["batch"]), 2)

    def test_flushes_on_the_interval(self):
        self.client(flush_interval=0.05).capture("u", "a")
        self.wait_for_requests(1)

    def test_retries_503_and_429_with_the_same_batch(self):
        vexo = self.client([(503, {"Retry-After": "0"}, {"success": False}), (429, {"Retry-After": "0"}, {"success": False})])
        vexo.capture("u", "a")
        vexo.flush()

        self.assertEqual(len(self.api.requests), 3)
        first, last = self.api.requests[0]["body"]["batch"][0], self.api.requests[2]["body"]["batch"][0]
        self.assertEqual(first["messageId"], last["messageId"])
        self.assertEqual(self.errors, [])

    def test_does_not_retry_a_4xx_and_reports_it(self):
        vexo = self.client([(401, {}, {"success": False, "message": "Permission denied"})])
        vexo.capture("u", "a")
        vexo.flush()

        self.assertEqual(len(self.api.requests), 1)
        self.assertEqual(len(self.errors), 1)
        self.assertEqual(self.errors[0].status, 401)
        self.assertEqual(len(self.errors[0].batch), 1)

    def test_gives_up_after_max_retries(self):
        vexo = self.client([(500, {"Retry-After": "0"}, {})] * 5, max_retries=2)
        vexo.capture("u", "a")
        vexo.flush()

        self.assertEqual(len(self.api.requests), 3)
        self.assertEqual(self.errors[0].status, 500)

    def test_reports_the_messages_the_server_dropped(self):
        dropped = {"success": True, "code": 200, "data": {"accepted": 1, "dropped": [{"index": 1, "reason": "too big"}]}}
        vexo = self.client([(200, {}, dropped)])
        vexo.capture("u", "ok")
        vexo.capture("u", "too_big")
        vexo.flush()

        self.assertEqual(self.errors[0].dropped, [{"message": self.api.requests[0]["body"]["batch"][1], "reason": "too big"}])

    def test_rejects_messages_without_a_distinct_id_and_after_shutdown(self):
        vexo = self.client()
        vexo.capture("", "a")
        vexo.shutdown()
        vexo.capture("u", "late")

        self.assertEqual(self.api.requests, [])
        self.assertEqual(len(self.errors), 2)

    def test_drops_messages_once_the_queue_is_full(self):
        vexo = self.client(max_queue_size=1)
        vexo.capture("u", "a")
        vexo.capture("u", "b")
        vexo.flush()

        self.assertEqual(len(self.api.requests[0]["body"]["batch"]), 1)
        self.assertIn("queue is full", str(self.errors[0]))

    def wait_for_requests(self, count, seconds=2.0):
        deadline = time.monotonic() + seconds
        while len(self.api.requests) < count and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(self.api.requests), count)


if __name__ == "__main__":
    unittest.main()
