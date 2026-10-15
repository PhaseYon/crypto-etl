"""Integration tests against real Kafka and a real S3-compatible store.

Start the services first (from the repo root):
    docker compose --env-file <env> up -d --wait kafka
    docker compose --env-file <env> --profile ui-test up -d --wait ui-test-s3
    docker compose --env-file <env> --profile ui-test up ui-test-s3-init
where <env> sets COMPOSE_PROJECT_NAME and host ports (see ui/README.md). Then:
    pytest api/tests -m integration
"""

import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from crypto_api.config import Settings
from crypto_api.main import create_app
from crypto_api.sources.base import TABLE_SPECS
from crypto_api.sources.kafka import KafkaMetadataSource, TopicUnavailableError
from crypto_api.sources.lake import DeltaLakeSource
from fastapi.testclient import TestClient
from lake_fixtures import utc_minute, write_bronze, write_gaps, write_gold, write_quarantine, write_silver
from pydantic import SecretStr

pytestmark = pytest.mark.integration

KAFKA = os.getenv("UI_IT_KAFKA_BOOTSTRAP", "localhost:59092")
S3_ENDPOINT = os.getenv("UI_IT_S3_ENDPOINT", "http://localhost:39000")
BUCKET = os.getenv("UI_IT_BUCKET", "ui-test-lake")
ACCESS_KEY = os.getenv("UI_IT_ACCESS_KEY", "uitestadmin")
SECRET_KEY = os.getenv("UI_IT_SECRET_KEY", "uitestsecret123")
NOW = datetime.now(UTC).replace(microsecond=0)


@pytest.fixture(scope="module")
def topic():
    """A fresh topic per run (2 partitions, 1h retention) with known messages; deleted afterwards."""
    name = f"ui-it-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    try:
        admin.list_topics(timeout=5)
    except Exception as e:
        pytest.fail(f"Kafka not reachable at {KAFKA} ({e}); see this module's docstring")
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=2, replication_factor=1, config={"retention.ms": "3600000"})]
    )
    futures[name].result(timeout=10)
    producer = Producer({"bootstrap.servers": KAFKA})
    for i in range(10):
        producer.produce(name, key=b"BTC-USD", value=f'{{"n": {i}}}'.encode(), partition=0)
    for i in range(4):
        producer.produce(name, key=b"ETH-USD", value=f'{{"n": {i}}}'.encode(), partition=1)
    assert producer.flush(10) == 0
    yield name
    admin.delete_topics([name])[name].result(timeout=10)


def storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        # Single test writer; no lock provider needed
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


@pytest.fixture(scope="module")
def lake_root():
    """Contract-shaped tables under a unique prefix in the test bucket."""
    root = f"s3://{BUCKET}/run-{uuid.uuid4().hex[:8]}"
    options = storage_options()
    try:
        write_bronze(root, [(0, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5)], NOW - timedelta(seconds=30), options)
        write_bronze(root, [(1, 0), (1, 1)], NOW, options)
    except Exception as e:
        pytest.fail(f"S3 store not reachable at {S3_ENDPOINT} ({e}); see this module's docstring")
    write_silver(root, ["ws"] * 9 + ["rest_backfill"], NOW, options)
    write_gold(
        root, {"BTC-USD": [utc_minute(m, NOW) for m in (3, 2, 1)], "ETH-USD": [utc_minute(1, NOW)]}, NOW, options
    )
    write_quarantine(root, ["bad side", "bad side", "missing price"], NOW, options)
    write_gaps(root, [("BTC-USD", 100, 104, NOW)], options)
    return root


def settings_for(topic: str, lake_root: str, **overrides) -> Settings:
    values = {
        "data_source": "real",
        "kafka_bootstrap": KAFKA,
        "kafka_topic": topic,
        "lake_uri": lake_root,
        "s3_endpoint": S3_ENDPOINT,
        "aws_access_key_id": SecretStr(ACCESS_KEY),
        "aws_secret_access_key": SecretStr(SECRET_KEY),
        "cache_ttl_seconds": 0,
        "source_timeout_seconds": 5,
        "PRODUCTS": "BTC-USD,ETH-USD",
    }
    values.update(overrides)
    return Settings(**values)


def test_kafka_watermarks_and_retention(topic):
    source = KafkaMetadataSource(KAFKA, topic, timeout_seconds=5)
    try:
        marks = source.watermarks()
        assert [(m.partition, m.low, m.high) for m in marks] == [(0, 0, 10), (1, 0, 4)]
        assert source.retention_ms() == 3_600_000
    finally:
        source.close()


def test_missing_topic_raises():
    source = KafkaMetadataSource(KAFKA, f"does-not-exist-{uuid.uuid4().hex[:6]}", timeout_seconds=3)
    try:
        with pytest.raises(TopicUnavailableError, match="not available: Broker: Unknown topic"):
            source.watermarks()
    finally:
        source.close()


def test_delta_reads_over_s3(lake_root):
    lake = DeltaLakeSource(lake_root, settings_for("unused", lake_root).storage_options())
    bronze = lake.table_snapshot(TABLE_SPECS["bronze"])
    assert (bronze.version, bronze.row_count) == (1, 8)
    assert bronze.newest_at == NOW
    assert lake.bronze_max_offsets() == {0: 5, 1: 1}
    assert [c.window_start for c in lake.ohlcv("BTC-USD", NOW - timedelta(minutes=10))] == [
        utc_minute(m, NOW) for m in (3, 2, 1)
    ]
    assert lake.quarantine(5).reasons == {"bad side": 2, "missing price": 1}
    assert [(g.product_id, g.missing_count) for g in lake.latest_gaps()] == [("BTC-USD", 5)]
    counts = lake.backfill_counts(NOW.date())
    assert (counts.total, counts.backfilled) == (10, 1)


def test_api_end_to_end(topic, lake_root):
    with TestClient(create_app(settings_for(topic, lake_root))) as client:
        pipeline = client.get("/api/pipeline").json()
        stages = {s["id"]: s for s in pipeline["stages"]}
        assert stages["kafka"]["status"] == "ok"
        assert stages["bronze"]["status"] == "ok"
        assert stages["gold"]["status"] == "ok"
        # Partition 0: high 10, bronze max offset 5 → lag 4; partition 1: high 4, max 1 → lag 2
        assert [(p["partition"], p["lag"]) for p in pipeline["lag"]["partitions"]] == [(0, 4), (1, 2)]
        tables = {t["id"]: t for t in pipeline["tables"]}
        assert tables["predictions"]["status"] == "inactive" and not tables["predictions"]["exists"]
        assert stages["ml"]["reason"] == "ML predictions not available yet"

        market = client.get("/api/market/ohlcv", params={"product": "BTC-USD", "minutes": 60}).json()
        assert market["status"] == "ok" and len(market["candles"]) == 3

        quality = client.get("/api/quality").json()
        assert quality["quarantine"]["total"] == 3
        assert quality["backfill"]["share"] == pytest.approx(0.1)


def test_unreachable_object_store_degrades_only_the_lake(topic, lake_root):
    settings = settings_for(topic, lake_root, s3_endpoint="http://127.0.0.1:1", source_timeout_seconds=3)
    started = time.monotonic()
    with TestClient(create_app(settings)) as client:
        pipeline = client.get("/api/pipeline").json()
    assert time.monotonic() - started < 15
    stages = {s["id"]: s["status"] for s in pipeline["stages"]}
    assert stages["kafka"] == "ok"
    assert stages["gold"] == "down"
    assert stages["bronze"] == "down"


def test_unreachable_kafka_degrades_only_kafka(lake_root):
    settings = settings_for("raw.trades", lake_root, kafka_bootstrap="127.0.0.1:1", source_timeout_seconds=3)
    with TestClient(create_app(settings)) as client:
        pipeline = client.get("/api/pipeline").json()
    stages = {s["id"]: s["status"] for s in pipeline["stages"]}
    assert stages["kafka"] == "down"
    assert stages["producer"] == "inactive"
    assert stages["gold"] == "ok"
