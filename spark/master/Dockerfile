FROM spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/spark/jobs

WORKDIR /opt/spark/jobs

EXPOSE 7077
EXPOSE 8080