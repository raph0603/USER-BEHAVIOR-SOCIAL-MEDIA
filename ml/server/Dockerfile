# AI-server image: serves the Stage-1 viral model over HTTP (FastAPI + uvicorn).
# Build context = the ml/ folder:   docker build -f server/Dockerfile -t ai-server ml
FROM python:3.12-slim

WORKDIR /app

# Model stack + web-serving extras.
COPY requirements-train.txt /tmp/requirements-train.txt
COPY server/requirements.txt /tmp/requirements-server.txt
RUN pip install --no-cache-dir -r /tmp/requirements-train.txt -r /tmp/requirements-server.txt

# Application code (whole ml/ tree so serve/, features/, preprocess/ are importable).
COPY . /app

# The trained model must be available at runtime; bind-mount it if it is not baked in:
#   docker run -p 8000:8000 -v $PWD/ml/models:/app/models ai-server
EXPOSE 8000
CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "8000"]
