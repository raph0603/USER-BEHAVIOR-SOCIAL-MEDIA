FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ml/requirements-api.txt ml/requirements-train.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-api.txt

COPY ml /app/ml

EXPOSE 8000

CMD ["uvicorn", "ml.service:app", "--host", "0.0.0.0", "--port", "8000"]
