from __future__ import annotations

import argparse
from collections.abc import Callable


ACTIVE_DAG_RUN_STATES = frozenset({"queued", "running"})
LOCK_DIR = "/tmp/user-behavior-lakehouse.pipeline.lock"
LOCK_GUARD = "/tmp/user-behavior-lakehouse.pipeline.lock.guard"


def parse_lock_owner(owner: str) -> tuple[str, str] | None:
    dag_id, separator, run_id = owner.partition("/")
    if not separator or not dag_id or not run_id:
        return None
    return dag_id, run_id


def lookup_dag_run_state(dag_id: str, run_id: str) -> str | None:
    from airflow import settings
    from airflow.models import DagRun

    session = settings.Session()
    try:
        row = (
            session.query(DagRun.state)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
            .one_or_none()
        )
        return None if row is None else str(row[0]).lower()
    finally:
        session.close()


def classify_lock_owner(
    owner: str,
    state_lookup: Callable[[str, str], str | None] = lookup_dag_run_state,
) -> str:
    parsed_owner = parse_lock_owner(owner)
    if parsed_owner is None:
        return "invalid"

    state = state_lookup(*parsed_owner)
    if state is None:
        return "missing"
    if state in ACTIVE_DAG_RUN_STATES:
        return "active"
    return f"terminal:{state}"


def acquire_pipeline_lock_command() -> str:
    return rf"""
    set -euo pipefail
    LOCK_DIR={LOCK_DIR}
    LOCK_GUARD={LOCK_GUARD}
    OWNER="${{AIRFLOW_CTX_DAG_ID}}/${{AIRFLOW_CTX_DAG_RUN_ID}}"
    POLL_SECONDS="${{PIPELINE_LOCK_POLL_SECONDS:-10}}"
    MAX_WAIT_SECONDS="${{PIPELINE_LOCK_MAX_WAIT_SECONDS:-7200}}"
    STALE_GRACE_SECONDS="${{PIPELINE_LOCK_STALE_GRACE_SECONDS:-30}}"
    OWNER_STATE_HELPER=/opt/airflow/dags/pipeline_lock.py

    for value in "$POLL_SECONDS" "$MAX_WAIT_SECONDS" "$STALE_GRACE_SECONDS"; do
      if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "Pipeline lock timing values must be non-negative integers"
        exit 1
      fi
    done
    if (( POLL_SECONDS < 1 || MAX_WAIT_SECONDS < 1 )); then
      echo "Pipeline lock poll and maximum wait values must be positive"
      exit 1
    fi

    MAX_ATTEMPTS=$(( (MAX_WAIT_SECONDS + POLL_SECONDS - 1) / POLL_SECONDS ))
    for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
      if docker exec \
        -e LOCK_OWNER="$OWNER" \
        -e LOCK_DIR="$LOCK_DIR" \
        -e LOCK_GUARD="$LOCK_GUARD" \
        spark-master /bin/bash -lc '
          set -euo pipefail
          exec 9>"$LOCK_GUARD"
          flock -x 9
          if mkdir "$LOCK_DIR" 2>/dev/null; then
            printf "%s\n" "$LOCK_OWNER" > "$LOCK_DIR/owner"
            date +%s > "$LOCK_DIR/acquired_at"
            exit 0
          fi
          CURRENT_OWNER=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
          [[ "$CURRENT_OWNER" == "$LOCK_OWNER" ]]
        '; then
        echo "Acquired shared pipeline lock for $OWNER"
        exit 0
      fi

      LOCK_INFO=$(docker exec \
        -e LOCK_DIR="$LOCK_DIR" \
        spark-master /bin/bash -lc '
          CURRENT_OWNER=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
          ACQUIRED_AT=$(cat "$LOCK_DIR/acquired_at" 2>/dev/null || \
            stat -c %Y "$LOCK_DIR" 2>/dev/null || echo 0)
          NOW=$(date +%s)
          AGE=$(( NOW - ACQUIRED_AT ))
          printf "%s|%s\n" "${{CURRENT_OWNER:-unknown}}" "$AGE"
        ')
      IFS='|' read -r CURRENT_OWNER LOCK_AGE <<< "$LOCK_INFO"

      if OWNER_STATE=$(python "$OWNER_STATE_HELPER" status "$CURRENT_OWNER"); then
        if [[ "$OWNER_STATE" == terminal:* || \
              "$OWNER_STATE" == "missing" || \
              "$OWNER_STATE" == "invalid" ]]; then
          if (( LOCK_AGE >= STALE_GRACE_SECONDS )); then
            if docker exec \
              -e EXPECTED_OWNER="$CURRENT_OWNER" \
              -e LOCK_DIR="$LOCK_DIR" \
              -e LOCK_GUARD="$LOCK_GUARD" \
              -e MIN_LOCK_AGE="$STALE_GRACE_SECONDS" \
              spark-master /bin/bash -lc '
                set -euo pipefail
                exec 9>"$LOCK_GUARD"
                flock -x 9
                CURRENT_OWNER=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
                ACQUIRED_AT=$(cat "$LOCK_DIR/acquired_at" 2>/dev/null || \
                  stat -c %Y "$LOCK_DIR" 2>/dev/null || echo 0)
                NOW=$(date +%s)
                AGE=$(( NOW - ACQUIRED_AT ))
                if [[ "$CURRENT_OWNER" == "$EXPECTED_OWNER" ]] && \
                   (( AGE >= MIN_LOCK_AGE )); then
                  rm -rf "$LOCK_DIR"
                  exit 0
                fi
                exit 1
              '; then
              echo "Reclaimed orphaned pipeline lock from $CURRENT_OWNER ($OWNER_STATE)"
              continue
            fi
          fi
        fi
      else
        OWNER_STATE="check-error"
      fi

      if (( attempt % 6 == 1 )); then
        echo "Pipeline busy with $CURRENT_OWNER ($OWNER_STATE, age=${{LOCK_AGE}}s); waiting..."
      fi
      sleep "$POLL_SECONDS"
    done

    echo "Timed out waiting for the shared pipeline lock"
    exit 1
    """


def release_pipeline_lock_command() -> str:
    return rf"""
    set -euo pipefail
    OWNER="${{AIRFLOW_CTX_DAG_ID}}/${{AIRFLOW_CTX_DAG_RUN_ID}}"
    LOCK_DIR={LOCK_DIR}
    LOCK_GUARD={LOCK_GUARD}
    if ! docker exec spark-master true >/dev/null 2>&1; then
      echo "spark-master is not running; pipeline lock is already unavailable"
      exit 0
    fi
    docker exec \
      -e LOCK_OWNER="$OWNER" \
      -e LOCK_DIR="$LOCK_DIR" \
      -e LOCK_GUARD="$LOCK_GUARD" \
      spark-master /bin/bash -lc '
        set -euo pipefail
        exec 9>"$LOCK_GUARD"
        flock -x 9
        CURRENT_OWNER=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
        if [[ "$CURRENT_OWNER" == "$LOCK_OWNER" ]]; then
          rm -rf "$LOCK_DIR"
          echo "Released shared pipeline lock for $LOCK_OWNER"
        else
          echo "Pipeline lock belongs to ${{CURRENT_OWNER:-nobody}}; nothing to release"
        fi
      '
    """


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("owner")
    args = parser.parse_args()

    if args.command == "status":
        print(classify_lock_owner(args.owner))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
