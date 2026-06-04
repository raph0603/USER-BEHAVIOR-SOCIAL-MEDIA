import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from playwright.sync_api import sync_playwright


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name, str(default))
    try:
        return float(value)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip() if value else default


def _load_schema(schema_path: str) -> str:
    path = Path(schema_path)
    return path.read_text(encoding="utf-8")


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")


def _wait_for_schema_registry(url: str, timeout_sec: int) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"Schema Registry not reachable at {url}")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "test-topic")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    schema_path = os.getenv("SCHEMA_PATH", "/app/schemas/playwright_event.avsc")
    schema_wait = _env_int("SCHEMA_REGISTRY_WAIT_SEC", 60)
    urls = os.getenv("PLAYWRIGHT_TARGET_URLS", "https://example.com").split(",")
    urls = [u.strip() for u in urls if u.strip()]
    user_count = _env_int("PLAYWRIGHT_USERS", 3)
    interval_sec = _env_float("EVENT_INTERVAL_SEC", 2.0)
    mode = _env_str("PRODUCER_MODE", "playwright").lower()
    max_events = _env_int("PRODUCER_MAX_EVENTS", 0)

    print("Waiting for Schema Registry...")
    _wait_for_schema_registry(schema_registry_url, schema_wait)
    print("Schema Registry ready")
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry_client, _load_schema(schema_path))
    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )

    def send_event(user_id: str, url: str, title: str | None, error: str | None, source: str) -> None:
        event = {
            "user_id": user_id,
            "url": url,
            "title": title,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "error": error,
        }
        producer.produce(
            topic=topic,
            key=user_id,
            value=event,
            on_delivery=_delivery_report,
        )
        producer.poll(0)

    def synthetic_loop() -> None:
        print("Producer mode: synthetic")
        counter = 0
        while True:
            for idx in range(user_count):
                target = random.choice(urls)
                send_event(
                    user_id=f"synthetic-user-{idx}",
                    url=target,
                    title="synthetic",
                    error=None,
                    source="synthetic",
                )
                counter += 1
                if max_events > 0 and counter >= max_events:
                    producer.flush()
                    print(f"Produced {counter} synthetic events (limit reached)")
                    return
            producer.flush()
            if counter % max(1, user_count * 5) == 0:
                print(f"Produced {counter} synthetic events")
            time.sleep(interval_sec)

    if mode != "playwright":
        synthetic_loop()
        return

    print("Producer mode: playwright")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"], timeout=30000)
            contexts = [browser.new_context() for _ in range(user_count)]
            pages = [ctx.new_page() for ctx in contexts]

            counter = 0
            while True:
                for idx, page in enumerate(pages):
                    target = random.choice(urls)
                    try:
                        page.goto(target, wait_until="domcontentloaded", timeout=30000)
                        title = page.title()
                        send_event(
                            user_id=f"pw-user-{idx}",
                            url=page.url,
                            title=title,
                            error=None,
                            source="playwright",
                        )
                    except Exception as exc:  # noqa: BLE001
                        send_event(
                            user_id=f"pw-user-{idx}",
                            url=target,
                            title=None,
                            error=str(exc),
                            source="playwright",
                        )
                    counter += 1
                producer.flush()
                if counter % max(1, user_count * 5) == 0:
                    print(f"Produced {counter} playwright events")
                time.sleep(interval_sec)
    except Exception as exc:  # noqa: BLE001
        print(f"Playwright failed, falling back to synthetic mode: {exc}")
        synthetic_loop()


if __name__ == "__main__":
    main()
