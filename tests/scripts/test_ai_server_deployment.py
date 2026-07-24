import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from ml.report_ui.generate_report import render_ollama


ROOT = Path(__file__).resolve().parents[2]


def test_compose_runs_the_published_ai_server_with_a_read_only_model_mount():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "  ai-server:" in compose
    assert "user-behavior-social-media-ai-server:${PROJECT_IMAGE_TAG:-latest}" in compose
    assert "${AI_MODEL_DIR:-./ml/models}:/app/models:ro" in compose
    assert "${AI_SERVER_PORT:-8000}:8000" in compose
    assert "AI_SERVER_TOKEN: ${AI_SERVER_TOKEN:-}" in compose
    assert "DASHBOARD_AI_SERVER_URL=${DASHBOARD_AI_SERVER_URL:-http://ai-server:8000}" in compose
    assert "DASHBOARD_AI_SERVER_TOKEN=${DASHBOARD_AI_SERVER_TOKEN:-${AI_SERVER_TOKEN:-}}" in compose
    assert "OLLAMA_BASE_URL: ${OLLAMA_BASE_URL:-http://host.docker.internal:11434}" in compose
    assert '"host.docker.internal:host-gateway"' in compose


def test_release_publishes_ai_server_and_packages_a_model_directory():
    workflow = (ROOT / ".github" / "workflows" / "publish-docker-hub.yml").read_text(
        encoding="utf-8"
    )
    bundle = (ROOT / "deployment" / "compose.bundle.yaml").read_text(encoding="utf-8")
    split = (ROOT / "deployment" / "compose.split.yaml").read_text(encoding="utf-8")

    assert "image: user-behavior-social-media-ai-server" in workflow
    assert "context: ./ml\n            dockerfile: ./ml/server/Dockerfile" in workflow
    assert 'mkdir -p "$BUNDLE_DIR/ml/models"' in workflow
    assert 'cp deployment/compose.split.yaml "$BUNDLE_DIR/compose.split.yaml"' in workflow
    assert "  ai-server:\n    build: !reset null" in bundle
    assert 'profiles: ["ml-server"]' in split


def test_ollama_backend_uses_the_configured_base_url(monkeypatch):
    response = MagicMock()
    response.__enter__.return_value.read.return_value = json.dumps(
        {"response": "generated report"}
    ).encode("utf-8")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama.internal:11434/")

    result = {
        "viral_score": 0.7,
        "label": "viral-likely",
        "top_factors": [],
        "suggestions": [],
    }
    with patch("urllib.request.urlopen", return_value=response) as urlopen:
        report = render_ollama(result, "en", "qwen2.5:3b")

    request = urlopen.call_args.args[0]
    assert request.full_url == "http://ollama.internal:11434/api/generate"
    assert report == "generated report"
