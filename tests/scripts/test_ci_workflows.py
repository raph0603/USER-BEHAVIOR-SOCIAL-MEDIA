from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"


def test_branch_workflows_call_the_shared_validation_suite():
    for filename, job_name in (("ci.yml", "pr-validation"), ("ci-main.yml", "main-validation")):
        source = (WORKFLOWS / filename).read_text(encoding="utf-8")
        assert f"{job_name}:" in source
        assert "uses: ./.github/workflows/validation.yml" in source


def test_pull_request_workflow_reports_the_required_aggregate_status():
    source = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
    gate = source.split("  required-pr-validation:", maxsplit=1)[1]

    assert "\n    name: pr-validation\n" in gate
    assert "\n    if: ${{ always() }}\n" in gate
    assert "\n    needs: pr-validation\n" in gate
    assert "VALIDATION_RESULT: ${{ needs.pr-validation.result }}" in gate
    assert 'if [ "$VALIDATION_RESULT" != "success" ]; then' in gate


def test_validation_suite_separates_all_required_gates():
    source = (WORKFLOWS / "validation.yml").read_text(encoding="utf-8")

    for job in ("static-unit", "contracts", "spark", "airflow", "e2e"):
        assert f"  {job}:" in source
    assert "tests/e2e/run_pipeline_reliability_e2e.sh" in source
    assert "test_lakehouse_dagbag" not in source
    assert "python -m unittest discover" in source
    assert "tests/scripts/test_engagement_snapshots.py" in source
    assert "tests/scripts/test_silver_post_features.py" in source
    assert "tests/scripts/test_experiment_reproducibility.py" in source
    assert "--ignore=tests/scripts/test_engagement_snapshots.py" not in source
    assert "--ignore=tests/scripts/test_silver_post_features.py" not in source


def test_e2e_compose_contains_only_deterministic_infrastructure():
    source = (ROOT / "tests" / "e2e" / "docker-compose.yml").read_text(encoding="utf-8")

    for service in ("minio", "kafka", "schema-registry", "spark", "dashboard"):
        assert f"  {service}:" in source
    assert "youtube-collector:" not in source
    assert "x-collector:" not in source
    assert "reddit-collector:" not in source
