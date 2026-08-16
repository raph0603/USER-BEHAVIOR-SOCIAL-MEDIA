from pathlib import Path

from ml.serve import explain_viral


def test_model_path_defaults_to_production_artifact(monkeypatch):
    monkeypatch.delenv(explain_viral.MODEL_PATH_ENV, raising=False)

    assert explain_viral.configured_model_path() == explain_viral.DEFAULT_MODEL_PATH


def test_model_path_can_be_selected_from_environment(monkeypatch):
    selected = Path("/app/models/stage1_multisource_audience_x90.joblib")
    monkeypatch.setenv(explain_viral.MODEL_PATH_ENV, str(selected))

    assert explain_viral.configured_model_path() == selected


def test_named_models_have_independent_paths(monkeypatch):
    legacy = Path("/models/legacy.joblib")
    x90 = Path("/models/audience-x90.joblib")
    monkeypatch.setenv("AI_MODEL_LEGACY_PATH", str(legacy))
    monkeypatch.setenv("AI_MODEL_X90_PATH", str(x90))

    assert explain_viral.configured_model_path("legacy") == legacy
    assert explain_viral.configured_model_path("audience-x90") == x90
