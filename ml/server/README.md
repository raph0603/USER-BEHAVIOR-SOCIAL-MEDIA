# AI-server (Task B)

REST API around the Stage-1 viral model. It exposes
`ml/serve/explain_viral.explain_post` over HTTP. The **API contract is the JSON
schema in `ml/HANDOFF.md` section 2** — consuming services rely on it.

## Endpoints

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/health` | — | `{"status": "ok", "model_loaded": bool}` |
| POST | `/predict` | `{"text": "...", "source": "youtube"}` | one explain_post JSON |
| POST | `/predict/batch` | `{"items": [{text, source}, ...]}` | list of explain_post JSON |

`source` ∈ `{"youtube", "x", "reddit", ""}`. Optional `audience` (float) is accepted.

Interactive docs once running: <http://localhost:8000/docs>.

## Run locally

```bash
cd ml
pip install -r requirements-train.txt -r server/requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

The model (`ml/models/stage1_multisource.joblib`) is loaded lazily on the first
prediction. If it is missing, `/predict` returns **503** with a clear message —
train it first with `python ml/run_pipeline.py`.

Quick check:

```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"text":"This EV has insane range, order today!","source":"x"}'
```

## Run with Docker

```bash
docker build -f server/Dockerfile -t ai-server ml
docker run -p 8000:8000 -v "$PWD/ml/models:/app/models" ai-server
```

## Tests (no model needed — `predict_fn` is stubbed)

```bash
pip install fastapi httpx pytest
pytest ml/server/test_app.py -q
```
