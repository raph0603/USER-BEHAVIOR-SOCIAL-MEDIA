FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/ .
COPY scripts/ /app/scripts/
COPY schemas/ /app/schemas/
RUN chmod +x /app/scripts/airflow_jobs_cli.py \
    && printf '%s\n' '#!/bin/sh' \
        'exec python /app/scripts/airflow_jobs_cli.py "$@"' \
        > /usr/local/bin/airflow-jobs \
    && chmod +x /usr/local/bin/airflow-jobs

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
