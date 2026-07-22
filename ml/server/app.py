"""Task B - AI-server (REST API) around the Stage-1 viral model.

Wraps ``ml/serve/explain_viral.explain_post`` behind HTTP so other services in the
learning platform can call it. The API contract IS the JSON schema documented in
ml/HANDOFF.md section 2 - this server does not change it, it only exposes it.

Endpoints
---------
    GET  /health          -> {"status": "ok", "model_loaded": bool}
    POST /predict         -> one post   -> the explain_post JSON
    POST /predict/batch   -> many posts -> list of explain_post JSON

Run
---
    uvicorn server.app:app --host 0.0.0.0 --port 8000    # from the ml/ folder
    # or:  python ml/server/app.py

The heavy model is loaded once, on first prediction (lazy singleton in
explain_viral). Call GET /health after startup to warm it up.
"""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Make the ml/ folder importable (so `from serve.explain_viral import ...` works).
ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

VALID_SOURCES = {"", "youtube", "x", "reddit"}


# --------------------------------------------------------------------------- #
# Request / response models
# --------------------------------------------------------------------------- #
class PredictRequest(BaseModel):
    text: str = Field(..., min_length=1, description="The social-media post text.")
    source: str = Field("", description='One of "youtube", "x", "reddit", or "".')
    audience: float | None = Field(None, description="Optional channel audience size.")


class BatchRequest(BaseModel):
    items: list[PredictRequest] = Field(..., min_length=1)


# --------------------------------------------------------------------------- #
# Prediction hook - real handler is imported lazily so importing this module
# (e.g. in tests) does not require the heavy ML stack or the model file.
# Tests override `predict_fn` with a stub.
# --------------------------------------------------------------------------- #
def _default_predict(text: str, source: str, audience: float | None) -> dict:
    from serve.explain_viral import explain_post  # noqa: PLC0415 - deliberate lazy import
    return explain_post(text, source, audience)


predict_fn = _default_predict


def _run(req: PredictRequest) -> dict:
    if req.source not in VALID_SOURCES:
        raise HTTPException(status_code=422, detail=f"source must be one of {sorted(VALID_SOURCES)}")
    try:
        return predict_fn(req.text, req.source, req.audience)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=f"Model not available: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - surface a clean 500 to the caller
        raise HTTPException(status_code=500, detail=f"Prediction failed: {exc}") from exc


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
app = FastAPI(
    title="Viral prediction AI-server",
    version="0.1.0",
    description="Exposes the Stage-1 viral prediction + explanation model over HTTP.",
)

# Allow the future web UI (any origin during dev) to call the API from the browser.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    """Liveness probe. `model_loaded` reflects whether the singleton is warm."""
    loaded = False
    try:
        from serve import explain_viral
        loaded = explain_viral._explainer is not None
    except Exception:  # noqa: BLE001 - health must never fail
        loaded = False
    return {"status": "ok", "model_loaded": loaded}


@app.post("/predict")
def predict(req: PredictRequest) -> dict:
    return _run(req)


@app.post("/predict/batch")
def predict_batch(req: BatchRequest) -> list[dict]:
    return [_run(item) for item in req.items]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server.app:app", host="0.0.0.0", port=8000, reload=False)


# --------------------------------------------------------------------------- #
# /report - prediction + rapport marketing genere par LLM (Task A), en un appel
# --------------------------------------------------------------------------- #
import os as _os  # noqa: E402

REPORT_MODEL = _os.environ.get("REPORT_MODEL", "qwen2.5:3b")
REPORT_BACKEND = _os.environ.get("REPORT_BACKEND", "ollama")


class ReportRequest(BaseModel):
    text: str = Field(..., min_length=1, description="The social-media post text.")
    source: str = Field("", description='One of "youtube", "x", "reddit", or "".')
    audience: float | None = Field(None, description="Optional channel audience size.")
    lang: str = Field("en", description='Report language: "en" or "vi".')


@app.post("/report")
def report(req: ReportRequest) -> dict:
    if req.source not in VALID_SOURCES:
        raise HTTPException(status_code=422, detail=f"source must be one of {sorted(VALID_SOURCES)}")
    if req.lang not in {"en", "vi"}:
        raise HTTPException(status_code=422, detail='lang must be "en" or "vi"')
    prediction = _run(PredictRequest(text=req.text, source=req.source, audience=req.audience))
    try:
        from report_ui.generate_report import generate  # noqa: PLC0415
        report_text = generate(prediction, REPORT_BACKEND, req.lang, REPORT_MODEL)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Report generation failed: {exc}") from exc
    return {"report": report_text, "prediction": prediction}
