# Lightweight image to run the Stage-1 AI training pipeline (run_pipeline.py).
# Code + data are bind-mounted at /workspace at run time, so this image only
# carries the Python dependencies. Used by the `ai-trainer` compose service.
FROM python:3.12-slim

WORKDIR /workspace

COPY ml/requirements-train.txt /tmp/requirements-train.txt
RUN pip install --no-cache-dir -r /tmp/requirements-train.txt

CMD ["python", "ml/run_pipeline.py", "--report"]
