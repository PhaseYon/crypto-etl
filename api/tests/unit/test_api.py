import json
from pathlib import Path

import pytest
from conftest import FakeKafka, FakeLake
from crypto_api.config import Settings
from crypto_api.main import create_app
from crypto_api.models import MarketResponse, MetaResponse, PipelineResponse, PredictionsResponse, QualityResponse
from fastapi.testclient import TestClient

OPENAPI_SNAPSHOT = Path(__file__).resolve().parents[3] / "ui" / "src" / "api" / "openapi.json"


@pytest.fixture
def mock_client():
    with TestClient(create_app(Settings(data_source="mock"))) as client:
        yield client


@pytest.mark.parametrize(
    ("path", "params", "model"),
    [
        ("/api/meta", None, MetaResponse),
        ("/api/pipeline", None, PipelineResponse),
        ("/api/market/ohlcv", {"product": "ETH-USD", "minutes": 120}, MarketResponse),
        ("/api/quality", None, QualityResponse),
        ("/api/ml/predictions", None, PredictionsResponse),
    ],
)
def test_mock_responses_validate(mock_client, path, params, model):
    response = mock_client.get(path, params=params)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    model.model_validate(response.json())


def test_mock_market_has_recent_candles(mock_client):
    body = mock_client.get("/api/market/ohlcv", params={"product": "BTC-USD", "minutes": 60}).json()
    assert body["status"] == "ok"
    assert 50 <= len(body["candles"]) <= 61
    times = [c["time"] for c in body["candles"]]
    assert times == sorted(times)


def test_degraded_mock_scenario(  # the dashboard must still render Kafka when the lake is down
):
    with TestClient(create_app(Settings(data_source="mock", mock_scenario="degraded"))) as client:
        body = client.get("/api/pipeline").json()
    by_id = {s["id"]: s["status"] for s in body["stages"]}
    assert by_id["kafka"] == "ok"
    assert by_id["gold"] == "down"


@pytest.mark.parametrize("product", ["DOGE-USD", "btc-usd", "BTC-USD' OR 1=1", ""])
def test_unknown_or_malformed_product_rejected(mock_client, product):
    assert mock_client.get("/api/market/ohlcv", params={"product": product}).status_code == 422


@pytest.mark.parametrize("minutes", [0, 14, 10_081])
def test_minutes_bounds(mock_client, minutes):
    response = mock_client.get("/api/market/ohlcv", params={"product": "BTC-USD", "minutes": minutes})
    assert response.status_code == 422


def test_every_endpoint_survives_all_sources_failing():
    lake = FakeLake()
    lake.error = OSError("down")
    app = create_app(Settings(source_timeout_seconds=0.5, cache_ttl_seconds=0), FakeKafka(error=OSError("down")), lake)
    with TestClient(app) as client:
        for path, params in [
            ("/api/pipeline", None),
            ("/api/quality", None),
            ("/api/ml/predictions", None),
            ("/api/market/ohlcv", {"product": "BTC-USD"}),
        ]:
            assert client.get(path, params=params).status_code == 200, path


def test_livez(mock_client):
    assert mock_client.get("/api/livez").json() == {"status": "ok"}


def test_openapi_snapshot_is_current():
    """The UI's TypeScript types are generated from ui/src/api/openapi.json.

    If this fails, the API schema changed: run `python api/scripts/export_openapi.py`
    and `npm run gen:api` in ui/, then commit both.
    """
    current = create_app(Settings()).openapi()
    assert json.loads(OPENAPI_SNAPSHOT.read_text()) == current


def test_snapshot_mode_builds_no_kafka_client(tmp_path):
    from datetime import UTC, datetime

    from crypto_api.config import Settings
    from crypto_api.main import build_sources
    from crypto_api.sources.base import NotDeployedKafka

    settings = Settings(data_source="real", lake_uri=str(tmp_path), snapshot_at=datetime(2026, 10, 22, tzinfo=UTC))
    kafka, _ = build_sources(settings)
    assert isinstance(kafka, NotDeployedKafka)
