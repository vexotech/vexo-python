"""Conformance driver: reads a scenario on stdin, drives the SDK, and prints JSON lines.

Protocol: vexo-sdk-conformance/README.md. Run: python3 run.py --driver "python3 conformance/driver.py"
"""
import json
import os
import sys
import threading
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from vexo import Vexo  # noqa: E402

OPTIONS = {"flushAt": ("flush_at", 1), "flushIntervalMs": ("flush_interval", 1000), "maxRetries": ("max_retries", 1),
           "maxQueueSize": ("max_queue_size", 1), "timeoutMs": ("timeout", 1000),
           "shutdownTimeoutMs": ("shutdown_timeout", 1000)}
lock = threading.Lock()


def output(line):
    with lock:  # on_error runs on the sending thread too
        sys.stdout.write(json.dumps(line) + "\n")
        sys.stdout.flush()


def on_error(error):
    output({"type": "error", "message": str(error), "status": getattr(error, "status", None)})


def main():
    scenario = json.load(sys.stdin)
    options = {name: scenario["options"][key] / divisor
               for key, (name, divisor) in OPTIONS.items() if key in scenario["options"]}
    for name in ("flush_at", "max_retries", "max_queue_size"):
        if name in options:
            options[name] = int(options[name])
    vexo = Vexo(scenario["apiKey"], host=scenario["host"], on_error=on_error, **options)
    for step in scenario["steps"]:
        start = time.monotonic()
        op = step["op"]
        if op == "capture":
            properties = step.get("properties")
            if step.get("poison"):
                properties = dict(properties or {})
                properties["poison"] = properties  # a circular reference: json.dumps refuses it
            if step.get("nan"):
                properties = dict(properties or {}, nan=float("nan"))
            timestamp = datetime.fromisoformat(step["at"].replace("Z", "+00:00")) if "at" in step else step.get("timestamp")
            kwargs = {"message_id": step["messageId"]} if "messageId" in step else {}
            vexo.capture(step["distinctId"], step["event"], properties, timestamp=timestamp, **kwargs)
        elif op == "identify":
            vexo.identify(step["distinctId"], step.get("traits"))
        elif op == "flush":
            vexo.flush()
        elif op == "shutdown":
            vexo.shutdown()
        elif op == "sleep":
            time.sleep(step["ms"] / 1000)
        elif op == "exit":
            os._exit(0)
        output({"type": "step", "op": op, "ms": (time.monotonic() - start) * 1000})


if __name__ == "__main__":
    main()
