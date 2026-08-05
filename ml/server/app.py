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
    allow_origins=["http://localhost:3000", "https://viral-insight-webapp-user-behavior.vercel.app"],
    allow_origin_regex=r"https://.*\.vercel\.app",    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------- #
# Rate limiting : max 60 requêtes / 60 s par IP (anti-abus)
# --------------------------------------------------------------------------- #
import time as _t  # noqa: E402
from collections import defaultdict as _dd, deque as _dq  # noqa: E402
from fastapi import Request  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402

_RATE_WINDOW = 60
_RATE_MAX = 60
_rate_hits: dict = _dd(_dq)


@app.middleware("http")
async def _rate_limit(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    fwd = request.headers.get("x-forwarded-for", "")
    ip = fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "unknown")
    now = _t.monotonic()
    hits = _rate_hits[ip]
    while hits and now - hits[0] > _RATE_WINDOW:
        hits.popleft()
    if len(hits) >= _RATE_MAX:
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded. Please slow down."})
    hits.append(now)
    return await call_next(request)


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
REPORT_GEN_MODEL = _os.environ.get("REPORT_GEN_MODEL", "qwen2.5:7b")


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
    if isinstance(prediction, dict):
        prediction["post_text"] = req.text
    # The report LLM should reason from the post + factors, NOT parrot the canned
    # `suggestions` (e.g. "Add a CTA") that make it recommend things already present.
    report_input = (
        {k: v for k, v in prediction.items() if k != "suggestions"}
        if isinstance(prediction, dict) else prediction
    )
    try:
        from report_ui.generate_report import generate  # noqa: PLC0415
        report_text = generate(report_input, REPORT_BACKEND, req.lang, REPORT_GEN_MODEL)    
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Report generation failed: {exc}") from exc
    return {"report": report_text, "prediction": prediction}


# --------------------------------------------------------------------------- #
# /barriers - EV adoption barrier radar (via Qwen, JSON output)
# --------------------------------------------------------------------------- #
import json as _json
import urllib.request as _urlreq

_BARRIERS = [
    ("range_anxiety", "Range anxiety"),
    ("charging_infrastructure", "Charging infrastructure"),
    ("battery_degradation", "Battery degradation"),
    ("safety_fire", "Safety / fire concerns"),
    ("price_incentives", "Price & incentives"),
    ("maintenance_cost", "Maintenance cost"),
]


class BarrierRequest(BaseModel):
    text: str = Field(..., min_length=1)


@app.post("/barriers")
def barriers(req: BarrierRequest) -> dict:
    keys = ", ".join(k for k, _ in _BARRIERS)
    prompt = (
        "You analyse a social-media post advertising an electric vehicle. "
        "For each of these six EV-adoption barriers, decide if the post ADDRESSES it "
        "(gives a concrete answer), MENTIONS it (raises it without answering), or does "
        "NOT mention it. Barriers: " + keys + ". Reply ONLY with JSON of shape "
        '{"barriers": {"range_anxiety": "addressed|mentioned|not_mentioned", ... all six ...}, '
        '"recommend": ["<up to two barrier keys most worth addressing>"]}. Post:\n' + req.text
    )
    body = _json.dumps({"model": REPORT_MODEL, "prompt": prompt, "stream": False, "format": "json"}).encode("utf-8")
    r = _urlreq.Request("http://localhost:11434/api/generate", data=body, headers={"Content-Type": "application/json"})
    try:
        with _urlreq.urlopen(r, timeout=120) as resp:
            data = _json.loads(_json.loads(resp.read())["response"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Barrier analysis failed: {exc}") from exc
    bmap = data.get("barriers", {}) if isinstance(data, dict) else {}
    out = []
    for key, label in _BARRIERS:
        status = str(bmap.get(key, "not_mentioned")).lower()
        if status not in ("addressed", "mentioned", "not_mentioned"):
            status = "not_mentioned"
        out.append({"key": key, "label": label, "status": status})
    rec = data.get("recommend", []) if isinstance(data, dict) else []
    return {"barriers": out, "recommend": rec if isinstance(rec, list) else []}






# --------------------------------------------------------------------------- #
# /greenwashing - greenwashing risk (via Qwen, JSON output)
# --------------------------------------------------------------------------- #
class GreenwashRequest(BaseModel):
    text: str = Field(..., min_length=1)
    lang: str = Field("en", description='"en" or "vi"')


@app.post("/greenwashing")
def greenwashing(req: GreenwashRequest) -> dict:
    prompt = (
        "You assess GREENWASHING risk in a social-media post advertising an electric vehicle. "
        "Greenwashing concerns ENVIRONMENTAL / sustainability claims ONLY — e.g. 'zero "
        "emissions', 'eco-friendly', 'green', 'carbon-neutral', 'clean energy', 'sustainable', "
        "'saves the planet'. Do NOT treat performance or business facts (range in km, charging "
        "speed, number of charging stations, price, number of customers, warranty) as "
        "environmental claims. Extract ONLY the environmental claims and any concrete evidence "
        "for them (numbers, certifications, lab tests, measurements). If there are NO "
        "environmental claims, return risk 'low' with empty claims and evidence. Otherwise, "
        "many environmental claims with little concrete evidence = higher risk. Reply ONLY with "
        'JSON of shape {"risk": "low|medium|high", "claims": ["..."], "evidence": ["..."], '
        '"note": "<one short sentence>"}. Write the "note" field in '
        + ("Vietnamese" if req.lang == "vi" else "English") + ". Post:\n" + req.text
    )
    body = _json.dumps({"model": REPORT_MODEL, "prompt": prompt, "stream": False, "format": "json"}).encode("utf-8")
    r = _urlreq.Request("http://localhost:11434/api/generate", data=body, headers={"Content-Type": "application/json"})
    try:
        with _urlreq.urlopen(r, timeout=120) as resp:
            data = _json.loads(_json.loads(resp.read())["response"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Greenwashing analysis failed: {exc}") from exc
    risk = str(data.get("risk", "medium")).lower() if isinstance(data, dict) else "medium"
    if risk not in ("low", "medium", "high"):
        risk = "medium"
    claims = data.get("claims", []) if isinstance(data, dict) else []
    evidence = data.get("evidence", []) if isinstance(data, dict) else []
    note = data.get("note", "") if isinstance(data, dict) else ""
    return {
        "risk": risk,
        "claims": claims if isinstance(claims, list) else [],
        "evidence": evidence if isinstance(evidence, list) else [],
        "note": str(note),
    }




# --------------------------------------------------------------------------- #
# /sentiment - likely audience reaction (via Qwen, JSON output)
# --------------------------------------------------------------------------- #
class SentimentRequest(BaseModel):
    text: str = Field(..., min_length=1)
    lang: str = Field("en", description='"en" or "vi"')


@app.post("/sentiment")
def sentiment(req: SentimentRequest) -> dict:
    prompt = (
        "You predict how a social-media AUDIENCE would likely REACT to a post advertising "
        "an electric vehicle, before it is published. Consider tone, credibility, and how "
        "EV-curious readers usually respond. Choose one overall reaction: positive, neutral, "
        "skeptical, or hostile. Reply ONLY with JSON of shape "
        '{"reaction": "positive|neutral|skeptical|hostile", "note": "<one short sentence>"}. '
        'Write the "note" in ' + ("Vietnamese" if req.lang == "vi" else "English") + ". Post:\n" + req.text
    )
    body = _json.dumps({"model": REPORT_MODEL, "prompt": prompt, "stream": False, "format": "json"}).encode("utf-8")
    r = _urlreq.Request("http://localhost:11434/api/generate", data=body, headers={"Content-Type": "application/json"})
    try:
        with _urlreq.urlopen(r, timeout=120) as resp:
            data = _json.loads(_json.loads(resp.read())["response"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Sentiment analysis failed: {exc}") from exc
    reaction = str(data.get("reaction", "neutral")).lower() if isinstance(data, dict) else "neutral"
    if reaction not in ("positive", "neutral", "skeptical", "hostile"):
        reaction = "neutral"
    note = data.get("note", "") if isinstance(data, dict) else ""
    return {"reaction": reaction, "note": str(note)}