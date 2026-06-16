FROM mcr.microsoft.com/playwright/python:v1.60.0-noble

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY producer.py /app/producer.py
COPY engagement.py /app/engagement.py
COPY insight_refresh.py /app/insight_refresh.py
COPY youtube_authors.py /app/youtube_authors.py

CMD ["python", "/app/producer.py"]
