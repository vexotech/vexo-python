import json
import os
import socket
import threading
import time
import unittest
import warnings
from datetime import datetime
from decimal import Decimal
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
                data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
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
        self.assertEqual(capture["timestamp"], 1794052800000)
        self.assertNotEqual(identify["messageId"], capture["messageId"])
        self.assertEqual(self.errors, [])

    def test_defaults_the_timestamp_to_now_in_integer_milliseconds(self):
        vexo = self.client()
        before = int(time.time() * 1000)
        vexo.capture("u", "a")
        vexo.capture("u", "b", timestamp=time.time() * 1000)
        vexo.flush()
        first, second = (m["timestamp"] for m in self.api.requests[0]["body"]["batch"])
        self.assertIsInstance(first, int)
        self.assertIsInstance(second, int)
        self.assertGreaterEqual(first, before)

    def test_sends_the_callers_message_id_unchanged(self):
        vexo = self.client()
        vexo.capture("u", "a", message_id="order-1234-paid")
        vexo.capture("u", "b")
        vexo.flush()
        own, generated = (m["messageId"] for m in self.api.requests[0]["body"]["batch"])
        self.assertEqual(own, "order-1234-paid")
        self.assertEqual(len(generated), 36)

    def test_counts_a_2xx_it_cannot_parse_as_accepted(self):
        vexo = self.client([(200, {}, b"<html>ok</html>")])
        vexo.capture("u", "a")
        vexo.flush()
        self.assertEqual(len(self.api.requests), 1)
        self.assertEqual(self.errors, [])

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

    def test_sends_values_json_cannot_encode_as_strings(self):
        vexo = self.client()
        vexo.capture("u", "priced", {"amount": Decimal("9.90"), "at": datetime(2026, 1, 2)})
        vexo.flush()
        self.assertEqual(self.api.requests[0]["body"]["batch"][0]["properties"], {"amount": "9.90", "at": "2026-01-02 00:00:00"})

    def test_drops_only_the_message_that_cannot_be_serialized(self):
        vexo = self.client()
        circular = {}
        circular["self"] = circular
        vexo.capture("u", "ok")
        vexo.capture("u", "circular", circular)
        vexo.flush()

        self.assertEqual([m["event"] for m in self.api.requests[0]["body"]["batch"]], ["ok"])
        self.assertEqual(len(self.errors), 1)
        self.assertIn("JSON", str(self.errors[0]))

    def test_sends_again_only_the_messages_the_server_reports_as_failed(self):
        failed = {"success": True, "code": 200, "data": {"accepted": 1, "dropped": [], "failed": [{"index": 1, "reason": "retry"}]}}
        vexo = self.client([(200, {}, failed)])
        vexo.capture("a", "stored")
        vexo.capture("b", "failed_once")
        started = time.monotonic()
        vexo.flush()

        self.assertEqual(len(self.api.requests), 2)
        resent = self.api.requests[1]["body"]["batch"]
        self.assertEqual([m["event"] for m in resent], ["failed_once"])
        self.assertEqual(resent[0]["messageId"], self.api.requests[0]["body"]["batch"][1]["messageId"])
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(self.errors, [])

    def test_stops_retrying_the_next_batches_once_one_batch_used_up_its_retries(self):
        vexo = self.client([(500, {"Retry-After": "0"}, {})] * 20, flush_at=1, max_retries=2)
        for i in range(3):
            vexo.capture("u", "e%d" % i)
        vexo.shutdown()

        # 3 attempts for the first batch, then 1 for each of the others.
        self.assertEqual(len(self.api.requests), 5)
        self.assertEqual(len(self.errors), 3)

    def test_shutdown_gives_up_at_shutdown_timeout_when_the_api_is_unreachable(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        self.errors = []
        vexo = Vexo("test-key", host="http://127.0.0.1:%d" % port, flush_at=1, flush_interval=60, timeout=0.5,
                    shutdown_timeout=1, on_error=self.errors.append)
        for i in range(5):
            vexo.capture("u", "e%d" % i)

        started = time.monotonic()
        vexo.shutdown()

        self.assertLess(time.monotonic() - started, 2.5)
        self.assertTrue(any("deadline" in str(e) for e in self.errors))

    @unittest.skipUnless(hasattr(os, "fork"), "needs fork")
    def test_a_forked_child_sends_from_its_own_thread(self):
        # macOS looks up system proxies through Objective-C, which isn't fork safe; read proxies from the environment.
        os.environ.setdefault("no_proxy", "*")
        vexo = self.client(flush_at=1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pid = os.fork()
        if pid == 0:
            vexo.capture("child", "from_child")
            deadline = time.monotonic() + 3
            while vexo._queue and time.monotonic() < deadline:
                time.sleep(0.01)
            time.sleep(0.2)
            os._exit(0)
        os.waitpid(pid, 0)
        self.wait_for_requests(1)
        self.assertEqual(self.api.requests[0]["body"]["batch"][0]["event"], "from_child")


if __name__ == "__main__":
    unittest.main()
