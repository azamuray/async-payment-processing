"""Run inside the Compose network using the command in README.md."""

import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from uuid import uuid4

import httpx

API = "http://api:8000/api/v1/payments"
RABBIT_API = "http://rabbitmq:15672/api"
CALLBACK_HOST = "payments-smoke:9000"
HEADERS = {"X-API-Key": os.environ["API_KEY"]}
calls = defaultdict(list)
lock = Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with lock:
            history = calls[self.path]
            history.append((time.monotonic(), event, self.headers.get("Idempotency-Key")))
            fail = self.path == "/fail" or len(history) < 3
        self.send_response(503 if fail else 204)
        self.end_headers()

    def log_message(self, *_):
        pass


def wait_for(probe, description, timeout=35):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.25)
    raise AssertionError(f"Timed out: {description}")


def call_history(path):
    with lock:
        return list(calls[path]) if len(calls[path]) >= 3 else None


def main():
    with (
        ThreadingHTTPServer(("0.0.0.0", 9000), Receiver) as receiver,
        httpx.Client(headers=HEADERS, timeout=5) as api,
        httpx.Client(base_url=RABBIT_API, auth=("payments", "payments"), timeout=5) as rabbit,
    ):
        Thread(target=receiver.serve_forever, daemon=True).start()
        try:
            check_service(api, rabbit)
        finally:
            receiver.shutdown()
    print("OK: authentication, concurrent idempotency, webhook retries, DLQ and duplicate delivery")


def check_service(api, rabbit):
    wait_for(
        lambda: rabbit.get("/queues/%2F/payments.new").status_code == 200,
        "payments.new declared",
    )
    assert httpx.get(f"{API}/{uuid4()}").status_code == 401
    assert httpx.post(API, json={}).status_code == 401
    assert api.get(f"{API}/{uuid4()}").status_code == 404

    exact_amount = "1234567890123456.78"
    body = {
        "amount": exact_amount,
        "currency": "RUB",
        "description": "Smoke test",
        "metadata": {"test": True, "amount": 1.25},
        "webhook_url": f"http://{CALLBACK_HOST}/recover",
    }
    key = {"Idempotency-Key": str(uuid4()), "Content-Type": "application/json"}
    assert api.post(API, json=body).status_code == 422
    assert api.post(API, json=body | {"amount": "1.001"}, headers=key).status_code == 422
    numeric_body = json.dumps(body).replace(f'"{exact_amount}"', exact_amount, 1)
    assert api.post(
        API,
        content=numeric_body.replace(exact_amount, "0.0100000000000000001", 1),
        headers=key,
    ).status_code == 422
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _: api.post(API, content=numeric_body, headers=key), range(8)))
    assert all(response.status_code == 202 for response in responses), [
        (response.status_code, response.text) for response in responses
    ]
    payment_ids = {response.json()["payment_id"] for response in responses}
    assert len(payment_ids) == 1
    payment_id = payment_ids.pop()
    replay = api.post(API, json=body, headers=key)
    assert replay.status_code == 202
    assert replay.json()["payment_id"] == payment_id
    assert api.post(API, json=body | {"amount": "126.50"}, headers=key).status_code == 409
    changed_metadata = body | {"metadata": {"test": 1, "amount": 1.25}}
    assert api.post(API, json=changed_metadata, headers=key).status_code == 409

    recovered = wait_for(lambda: call_history("/recover"), "webhook succeeds on third attempt")
    payment = api.get(f"{API}/{payment_id}").raise_for_status().json()
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["amount"] == exact_amount
    assert payment["metadata"] == body["metadata"]
    check_webhook(recovered, payment)
    event_id = recovered[0][1]["event_id"]

    # Simulate a repeated publish after the relay lost its database commit.
    duplicate = rabbit.post(
        "/exchanges/%2F/payments/publish",
        json={
            "properties": {"content_type": "application/json", "message_id": event_id},
            "routing_key": "payments.new",
            "payload": json.dumps({"event_id": event_id, "payment_id": payment_id}),
            "payload_encoding": "string",
        },
    )
    assert duplicate.raise_for_status().json()["routed"]

    failed = api.post(
        API,
        json=body | {"webhook_url": f"http://{CALLBACK_HOST}/fail"},
        headers={"Idempotency-Key": str(uuid4())},
    ).raise_for_status().json()
    failed_id = failed["payment_id"]

    def find_dead_letter():
        response = rabbit.post(
            "/queues/%2F/payments.dlq/get",
            json={"count": 100, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        return next(
            (
                item for item in response.raise_for_status().json()
                if json.loads(item["payload"])["payment_id"] == failed_id
            ),
            None,
        )

    dead_letter = wait_for(find_dead_letter, "event reaches the DLQ")
    assert dead_letter["properties"]["headers"]["x-death"][0]["reason"] == "rejected"
    failed_payment = api.get(f"{API}/{failed_id}").raise_for_status().json()
    check_webhook(call_history("/fail"), failed_payment)
    assert call_history("/recover") == recovered
    assert api.get(f"{API}/{payment_id}").raise_for_status().json() == payment


def check_webhook(history, payment):
    assert len(history) == 3
    first, second, third = history
    assert second[0] - first[0] >= 0.9
    assert third[0] - second[0] >= 1.9
    assert first[1] == second[1] == third[1]
    assert first[2] == second[2] == third[2] == first[1]["event_id"]
    for field in ("payment_id", "status", "amount", "currency", "processed_at"):
        assert first[1][field] == payment[field]


if __name__ == "__main__":
    main()
