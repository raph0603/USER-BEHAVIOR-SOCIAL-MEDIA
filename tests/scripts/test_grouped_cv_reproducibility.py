import json
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Required for pytest to resolve common.*
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
ML_ROOT = PROJECT_ROOT / "ml"
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from ml.reproducibility_cli import validate_artifacts
from common.reproducibility import fingerprint, file_sha256

def test_grouped_cv_invariants(tmp_path):
    # Mock valid data
    evaluation_protocol = {
        "method": "StratifiedGroupKFold",
    }
    evaluation_protocol["evaluation_protocol_fingerprint"] = fingerprint(evaluation_protocol)
    
    cv_folds_manifest = {
        "folds": {"1": {"test_size": 2}, "2": {"test_size": 2}}
    }
    cv_folds_manifest["evaluation_folds_fingerprint"] = fingerprint(cv_folds_manifest)
    
    oof_df = pd.DataFrame({
        "example_id": ["id1", "id2", "id3", "id4"],
        "source": ["youtube"] * 4,
        "author_hash": ["a1", "a1", "a2", "a2"],
        "viral": [1, 0, 1, 0],
        "outer_fold": [1, 1, 2, 2],
        "raw_probability": [0.9, 0.1, 0.8, 0.2],
        "calibrated_probability": [0.95, 0.05, 0.85, 0.15],
        "classification_threshold": [0.5] * 4,
        "predicted_label": [True, False, True, False],
        "dataset_version": ["v1"] * 4,
        "virality_contract_fingerprint": ["xyz"] * 4,
        "evaluation_protocol_fingerprint": [evaluation_protocol["evaluation_protocol_fingerprint"]] * 4,
        "evaluation_folds_fingerprint": [cv_folds_manifest["evaluation_folds_fingerprint"]] * 4,
    })
    
    oof_path = tmp_path / "oof.parquet"
    oof_df.to_parquet(oof_path, index=False)
    
    eval_payload = {
        "metrics": {"f1": 1.0}
    }
    # Fix eval fingerprint
    from ml.reproducibility_cli import _evaluation_identity
    eval_payload["evaluation_fingerprint"] = fingerprint(_evaluation_identity(eval_payload))
    eval_path = tmp_path / "eval.json"
    eval_path.write_text(json.dumps(eval_payload))
    
    dataset_manifest = {
        "iceberg_snapshots_json": {"table": 1},
        "schema_version": "1.0",
        "gold_snapshot_id": 1,
    }
    
    from ml.reproducibility_cli import _dataset_fingerprint
    ds_fp = _dataset_fingerprint(dataset_manifest)
    dataset_manifest["dataset_fingerprint"] = ds_fp
    
    from common.reproducibility import manifest_sha256
    man_fp = manifest_sha256(dataset_manifest)
    dataset_manifest["manifest_sha256"] = man_fp
    
    lineage = {
        "dataset_version": "v1",
        "silver_snapshot_ids": {"table": 1},
        "gold_snapshot_id": 1,
        "dataset_fingerprint": ds_fp,
        "manifest_sha256": man_fp,
        "git_commit": "0123456789abcdef0123456789abcdef01234567",
        "environment_fingerprint": "fp_env",
        "training_config_fingerprint": "fp_conf",
        "evaluation_protocol_fingerprint": evaluation_protocol["evaluation_protocol_fingerprint"],
        "evaluation_folds_fingerprint": cv_folds_manifest["evaluation_folds_fingerprint"],
        "evaluation_fingerprint": eval_payload["evaluation_fingerprint"],
        "oof_predictions_sha256": file_sha256(oof_path),
        "metrics_sha256": file_sha256(eval_path),
        "model_sha256": "mod",
    }
    
    env_manifest = {
        "environment_fingerprint": "fp_env",
        "code": {"git_commit": "0123456789abcdef0123456789abcdef01234567"}
    }
    
    training_config = {
        "training_config_fingerprint": "fp_conf",
        "feature_schema": {"model_columns": ["col"]}
    }
    
    bundle = {
        "features": ["col"],
        "lineage": lineage
    }
    
    from unittest.mock import patch
    
    with patch("ml.reproducibility_cli.validate_environment_manifest"), \
         patch("ml.reproducibility_cli.validate_training_config"), \
         patch("ml.reproducibility_cli.validate_lineage_match"):
        # 1. Valid Check
        res = validate_artifacts(
            dataset_manifest=dataset_manifest,
            environment_manifest=env_manifest,
            training_config=training_config,
            split_manifest=None,
            evaluation_protocol=evaluation_protocol,
            cv_folds_manifest=cv_folds_manifest,
            oof_predictions_path=oof_path,
            metrics_path=eval_path,
            lineage=lineage,
            bundle=bundle,
            model_sha256="mod",
            evaluation=eval_payload
        )
        for k, (passed, msg) in res.items():
            assert passed is True, f"{k} failed: {msg}"
            
        # 2. Tampered Protocol Fingerprint
        ep_bad = dict(evaluation_protocol)
        ep_bad["evaluation_protocol_fingerprint"] = "bad"
        res = validate_artifacts(
            dataset_manifest=dataset_manifest,
            environment_manifest=env_manifest,
            training_config=training_config,
            split_manifest=None,
            evaluation_protocol=ep_bad,
            cv_folds_manifest=cv_folds_manifest,
            oof_predictions_path=oof_path,
            metrics_path=eval_path,
            lineage=lineage,
            bundle=bundle,
            model_sha256="mod",
            evaluation=eval_payload
        )
        assert res["Grouped CV Fingerprints"][0] is False
        assert "tampered" in res["Grouped CV Fingerprints"][1]
        
        # 3. OOF Invariants: Missing values
        oof_bad = oof_df.copy()
        oof_bad.loc[0, "raw_probability"] = np.nan
        oof_bad.to_parquet(oof_path, index=False)
        res = validate_artifacts(
            dataset_manifest=dataset_manifest,
            environment_manifest=env_manifest,
            training_config=training_config,
            split_manifest=None,
            evaluation_protocol=evaluation_protocol,
            cv_folds_manifest=cv_folds_manifest,
            oof_predictions_path=oof_path,
            metrics_path=eval_path,
            lineage=lineage,
            bundle=bundle,
            model_sha256="mod",
            evaluation=eval_payload
        )
        assert res["Grouped CV Fingerprints"][0] is False
        assert "OOF parquet SHA-256 mismatch" in res["Grouped CV Fingerprints"][1]
