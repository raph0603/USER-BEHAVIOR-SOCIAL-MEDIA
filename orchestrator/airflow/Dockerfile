FROM docker:27-cli AS docker-cli

FROM apache/airflow:2.9.3-python3.11

USER root

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose

RUN chmod +x /usr/local/bin/docker /usr/local/lib/docker/cli-plugins/docker-compose

USER airflow
