"""HTTP inference service for the Stage-1 virality model."""
from __future__ import annotations

import logging
import os
import secrets
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

LOGGER = logging.getLogger(__name__)
Source = Literal["youtube", "x", "reddit", ""]


class Predictor(Protocol):
    def explain(
        self,
        text: str,
        source: str = "",
        audience: float | None = None,
    ) -> dict[str, Any]: ...


class PredictionRequest(BaseModel):
    text: str = Field(min_length=1, max_length=100_000)
    source: Source = ""
    audience: float | None = Field(default=None, ge=0)


class BatchPredictionRequest(BaseModel):
    items: list[PredictionRequest] = Field(min_length=1, max_length=100)


class FactorResponse(BaseModel):
    feature: str
    label: str
    value: float | None
    contribution: float
    direction: Literal["up", "down"]


class PredictionResponse(BaseModel):
    viral_score: float = Field(ge=0, le=1)
    label: Literal["viral-likely", "not-viral"]
    confidence: float = Field(ge=0, le=1)
    top_factors: list[FactorResponse]
    explanation_text: str
    suggestions: list[str]


class BatchPredictionResponse(BaseModel):
    items: list[PredictionResponse]


class HealthResponse(BaseModel):
    status: Literal["ok"]
    model: Literal["ready"]


def _load_predictor() -> Predictor:
    # Import lazily so liveness and API-contract tests do not import the heavy
    # numerical stack before the application lifespan starts.
    from ml.serve.explain_viral import ViralExplainer

    return ViralExplainer()


def create_app(
    predictor_loader: Callable[[], Predictor] = _load_predictor,
) -> FastAPI:
    @asynccontextmanager
    async def predictor_lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.predictor = None
        application.state.model_error = None
        try:
            application.state.predictor = predictor_loader()
        except Exception as exc:  # Keep the API observable while artifacts are absent.
            application.state.model_error = str(exc)
            LOGGER.exception("Unable to load the ML model")
        yield
        application.state.predictor = None

    app = FastAPI(
        title="User Behavior Social Media ML API",
        version="1.0.0",
        lifespan=predictor_lifespan,
    )

    def authorize(authorization: str | None = Header(default=None)) -> None:
        configured_token = os.getenv("ML_API_TOKEN", "")
        if not configured_token:
            return
        expected = f"Bearer {configured_token}"
        if authorization is None or not secrets.compare_digest(authorization, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid ML API token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def ready_predictor() -> Predictor:
        predictor = app.state.predictor
        if predictor is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ML model is not ready",
            )
        return predictor

    @app.get("/health", response_model=HealthResponse)
    def health(_: None = Depends(authorize)) -> HealthResponse:
        ready_predictor()
        return HealthResponse(status="ok", model="ready")

    @app.post("/predict", response_model=PredictionResponse)
    def predict(
        request: PredictionRequest,
        _: None = Depends(authorize),
    ) -> dict[str, Any]:
        predictor = ready_predictor()
        return predictor.explain(request.text, request.source, request.audience)

    @app.post("/predict/batch", response_model=BatchPredictionResponse)
    def predict_batch(
        request: BatchPredictionRequest,
        _: None = Depends(authorize),
    ) -> dict[str, list[dict[str, Any]]]:
        predictor = ready_predictor()
        return {
            "items": [
                predictor.explain(item.text, item.source, item.audience)
                for item in request.items
            ]
        }

    return app


app = create_app()
