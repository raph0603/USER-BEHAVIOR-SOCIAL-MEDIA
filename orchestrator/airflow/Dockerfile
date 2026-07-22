FROM docker:27-cli AS docker-cli

FROM apache/airflow:2.9.3-python3.11

USER root

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose

RUN chmod +x /usr/local/bin/docker /usr/local/lib/docker/cli-plugins/docker-compose

COPY orchestrator/dags/ /opt/airflow/dags/
COPY docker-compose.yml /workspace/docker-compose.yml
COPY .env.example /workspace/.env.example
COPY dashboard/ /workspace/dashboard/
COPY playwright/ /workspace/playwright/
COPY spark/ /workspace/spark/
COPY common/ /workspace/common/
COPY schemas/ /workspace/schemas/
COPY scripts/ /workspace/scripts/
COPY tests/spark/ /workspace/tests/spark/
COPY tests/airflow/ /workspace/tests/airflow/
COPY orchestrator/airflow/ /workspace/orchestrator/airflow/

RUN mkdir -p /opt/airflow/logs /workspace/data/collector-state /workspace/data/insight-refresh /workspace/data/balancing /workspace/data/x-runtime \
    && printf '%s\n' '#!/bin/sh' 'exec python /workspace/scripts/stack_transfer_cli.py "$@"' \
        > /usr/local/bin/stack-transfer \
    && chmod +x /usr/local/bin/stack-transfer

USER airflow
