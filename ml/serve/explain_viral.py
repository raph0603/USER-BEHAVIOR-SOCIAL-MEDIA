"""Explainable viral prediction (Stage 1) — the novelty: not just a score, but WHY.

Loads the fused multi-source model (stage1_multisource.joblib), rebuilds the exact
training features for one post, predicts P(viral), then uses XGBoost per-prediction
SHAP contributions to surface the top drivers as human-readable factors. Emits the
structured JSON the dashboard parses:
  {viral_score, label, confidence, top_factors[], explanation_text, suggestions[]}
"""
from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd
import xgboost as xgb

ML_ROOT = Path(__file__).resolve().parents[1]
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))

from features.rhetorical_roles import RoleFeaturizer
from preprocess.build_dataset import add_text_features, clean_text

MODEL_PATH = ML_ROOT / "models" / "stage1_multisource.joblib"
DECISION_THRESHOLD = 0.50
TOP_K = 5

# Map raw feature names -> human-readable Vietnamese labels for the explanation.
FEATURE_LABELS = {
    "content_score": "Nội dung/chủ đề bài viết",
    "char_count": "Độ dài bài (ký tự)",
    "word_count": "Số từ",
    "has_question": "Có câu hỏi",
    "is_vietnamese": "Viết bằng tiếng Việt",
    "f_word": "Từ vựng khó",
    "f_sent": "Câu dài",
    "f_clause": "Câu nhiều mệnh đề",
    "f_info": "Mật độ thuật ngữ kỹ thuật",
    "f_visual": "Yếu tố thị giác (CHỮ HOA/!!!/emoji/#)",
    "cognitive_friction_score": "Độ khó đọc tổng thể",
    "role_diversity": "Đa dạng vai trò marketing",
    "role_n_segments": "Số đoạn trong bài",
}
_ROLE_LABELS = {
    "cta": "kêu gọi hành động (CTA)",
    "hook": "câu hook mở đầu",
    "proof": "bằng chứng/số liệu",
    "social_proof": "social proof (người khác đã dùng)",
    "pain_point": "nêu vấn đề người dùng",
    "urgency": "tính khẩn cấp",
}


def _label_for(feature: str) -> str:
    if feature in FEATURE_LABELS:
        return FEATURE_LABELS[feature]
    if feature.startswith("src_"):
        return f"Nền tảng {feature[4:]}"
    for role, name in _ROLE_LABELS.items():
        if feature.endswith(role):
            kind = "Tỉ lệ" if "ratio" in feature else "Số đoạn"
            return f"{kind} {name}"
    return feature


class ViralExplainer:
    def __init__(self, model_path: Path = MODEL_PATH):
        bundle = joblib.load(model_path)
        self.model = bundle["model"]
        self.features = bundle["features"]
        if "content_model" in bundle:
            self.content_model = bundle["content_model"]
        else:  # BERT backend: rebuild from the saved model folder
            from features.bert_content import BertContentModel
            self.content_model = BertContentModel(bundle["content_model_dir"])
        self.roles = RoleFeaturizer()

    def _feature_row(self, text: str, source: str):
        cleaned = clean_text(text)
        df = add_text_features(pd.DataFrame({"clean_text": [cleaned]}))
        for name, value in self.roles.post_features(cleaned).items():
            df[name] = value
        df["content_score"] = self.content_model.predict_proba([cleaned])[:, 1]
        src = (source or "").strip().lower()
        for col in self.features:
            if col.startswith("src_"):
                df[col] = 1.0 if col == f"src_{src}" else 0.0
        return df.reindex(columns=self.features, fill_value=0.0).astype(float)

    def _contributions(self, X: pd.DataFrame) -> pd.Series:
        contribs = self.model.get_booster().predict(xgb.DMatrix(X), pred_contribs=True)
        return pd.Series(contribs[0][:-1], index=self.features)  # drop bias term

    def _suggestions(self, X: pd.DataFrame, is_viral: int) -> list[str]:
        row = X.iloc[0]
        tips = []
        if row.get("role_n_cta", 0) == 0:
            tips.append("Thêm lời kêu gọi hành động (CTA) rõ ràng.")
        if row.get("role_n_hook", 0) == 0:
            tips.append("Mở đầu bằng một câu hook gây chú ý.")
        if row.get("role_n_proof", 0) == 0:
            tips.append("Bổ sung số liệu/bằng chứng cụ thể.")
        if row.get("cognitive_friction_score", 0) >= 0.5:
            tips.append("Giảm độ khó đọc: câu ngắn hơn, ít thuật ngữ.")
        return tips[:3]

    def explain(self, text: str, source: str = "", threshold: float = DECISION_THRESHOLD) -> dict:
        X = self._feature_row(text, source)
        score = float(self.model.predict_proba(X)[0, 1])
        contribs = self._contributions(X)

        top = contribs.reindex(contribs.abs().sort_values(ascending=False).index).head(TOP_K)
        top_factors = [
            {
                "feature": feat,
                "label": _label_for(feat),
                "value": None if pd.isna(X.iloc[0][feat]) else round(float(X.iloc[0][feat]), 4),
                "contribution": round(float(c), 4),
                "direction": "tăng" if c > 0 else "giảm",
            }
            for feat, c in top.items()
        ]
        up = [f["label"] for f in top_factors if f["direction"] == "tăng"][:3]
        down = [f["label"] for f in top_factors if f["direction"] == "giảm"][:3]
        is_viral = int(score >= threshold)

        parts = [f"Dự đoán: {'CÓ khả năng viral' if is_viral else 'ÍT khả năng viral'} (xác suất {score:.0%})."]
        if up:
            parts.append("Yếu tố làm TĂNG: " + ", ".join(up) + ".")
        if down:
            parts.append("Yếu tố làm GIẢM: " + ", ".join(down) + ".")

        return {
            "viral_score": round(score, 3),
            "label": "viral-likely" if is_viral else "not-viral",
            "confidence": round(2 * abs(score - 0.5), 3),
            "top_factors": top_factors,
            "explanation_text": " ".join(parts),
            "suggestions": self._suggestions(X, is_viral),
        }


_explainer: ViralExplainer | None = None


def explain_post(text: str, source: str = "") -> dict:
    global _explainer
    if _explainer is None:
        _explainer = ViralExplainer()
    return _explainer.explain(text, source)


if __name__ == "__main__":
    import json

    samples = [
        ("This EV has insane range. Over 300,000 drivers already switched. Limited offer — order today!", "x"),
        ("Pin xe điện hết giữa đường, chờ 3 tiếng mới có cứu hộ.", "reddit"),
    ]
    for text, src in samples:
        print(json.dumps(explain_post(text, src), ensure_ascii=False, indent=2))
        print("-" * 60)
