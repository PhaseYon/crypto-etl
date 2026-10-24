"""FastAPI app: read-only endpoints under /api for the pipeline dashboard."""

import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from . import __version__
from .config import Settings, get_settings
from .models import (
    LivenessResponse,
    MarketResponse,
    MetaResponse,
    PipelineResponse,
    PredictionsResponse,
    QualityResponse,
)
from .runner import SourceRunner
from .services import PipelineService
from .sources.base import KafkaSource, LakeSource, NotDeployedKafka

PRODUCT_PATTERN = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$")
SNAPSHOT_MAX_AGE_SECONDS = 300


def get_service(request: Request) -> PipelineService:
    return request.app.state.service


# Module level so type checkers treat it as a type alias (inside a function it's just a variable)
Service = Annotated[PipelineService, Depends(get_service)]


def build_sources(settings: Settings) -> tuple[KafkaSource, LakeSource]:
    if settings.data_source == "mock":
        from .sources.mock import MockKafkaSource, MockLakeSource

        return MockKafkaSource(), MockLakeSource(settings.products, degraded=settings.mock_scenario == "degraded")
    from .sources.lake import DeltaLakeSource

    lake = DeltaLakeSource(settings.lake_root, settings.storage_options())
    if settings.snapshot_at is not None:
        # A snapshot has no broker; don't start a client that would retry connecting forever
        return NotDeployedKafka(), lake
    from .sources.kafka import KafkaMetadataSource

    kafka = KafkaMetadataSource(settings.kafka_bootstrap, settings.kafka_topic, settings.source_timeout_seconds)
    return kafka, lake


def create_app(
    settings: Settings | None = None,
    kafka: KafkaSource | None = None,
    lake: LakeSource | None = None,
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        real_kafka, real_lake = (kafka, lake) if kafka and lake else build_sources(settings)
        runner = SourceRunner(settings.source_timeout_seconds, settings.cache_ttl_seconds)
        app.state.service = PipelineService(settings, real_kafka, real_lake, runner)
        try:
            yield
        finally:
            runner.shutdown()
            close = getattr(real_kafka, "close", None)
            if close:
                close()

    app = FastAPI(
        title="crypto-etl dashboard API",
        version=__version__,
        lifespan=lifespan,
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        redoc_url=None,
    )
    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["GET"])

    # Live responses change every poll, so they aren't cached downstream (the server-side cache handles
    # load). A snapshot never changes, so a CDN may serve it without calling the API.
    cache_control = "no-store" if settings.snapshot_at is None else f"public, max-age={SNAPSHOT_MAX_AGE_SECONDS}"

    @app.middleware("http")
    async def set_cache_control(request: Request, call_next):  # type: ignore[no-untyped-def]
        response: Response = await call_next(request)
        response.headers["Cache-Control"] = cache_control
        return response

    @app.get("/api/livez", response_model=LivenessResponse, tags=["ops"])
    async def livez() -> LivenessResponse:
        """Liveness only: the API process is up. Source health is reported by /api/pipeline."""
        return LivenessResponse(status="ok")

    @app.get("/api/meta", response_model=MetaResponse, tags=["dashboard"])
    async def meta(svc: Service) -> MetaResponse:
        return svc.meta()

    @app.get("/api/pipeline", response_model=PipelineResponse, tags=["dashboard"])
    async def pipeline(svc: Service) -> PipelineResponse:
        return await svc.pipeline()

    @app.get("/api/market/ohlcv", response_model=MarketResponse, tags=["dashboard"])
    async def ohlcv(
        svc: Service,
        product: Annotated[str, Query(description="Product id, e.g. BTC-USD")],
        minutes: Annotated[int, Query(ge=15, le=10_080, description="Look-back window")] = 360,
    ) -> MarketResponse:
        # Product ids end up in a lake filter: accept only configured, well-formed ids
        if not PRODUCT_PATTERN.match(product) or product not in settings.products:
            raise HTTPException(
                status_code=422, detail=f"unknown product {product!r}; expected one of {settings.products}"
            )
        return await svc.market(product, minutes)

    @app.get("/api/quality", response_model=QualityResponse, tags=["dashboard"])
    async def quality(svc: Service) -> QualityResponse:
        return await svc.quality()

    @app.get("/api/ml/predictions", response_model=PredictionsResponse, tags=["dashboard"])
    async def predictions(svc: Service) -> PredictionsResponse:
        return await svc.predictions()

    return app


app = create_app()
