FROM mcr.microsoft.com/playwright/python:v1.60.0-noble

WORKDIR /app

COPY playwright/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY playwright/producer.py /app/producer.py
COPY playwright/engagement.py /app/engagement.py
COPY playwright/insight_refresh.py /app/insight_refresh.py
COPY playwright/youtube_authors.py /app/youtube_authors.py
COPY schemas/ /app/schemas/
COPY API/ /app/api/

RUN mkdir -p /app/state /app/insight-refresh /app/x-runtime /app/api/yt_raw_json

CMD ["python", "-u", "/app/producer.py"]
