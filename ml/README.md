# `ml/` — Dự đoán & giải thích viral (Stage 1)

Mô hình dự đoán một bài post social (EV) có khả năng **viral** hay không, **kèm lý do** (explainable) — phục vụ gợi ý nội dung marketing. Đa nguồn: **YouTube · X · Reddit**.

## Luồng pipeline

```
filtered_events.csv (export từ Silver lakehouse)
        │
        ▼  preprocess/build_dataset.py
  clean text → feature thống nhất (content) + nhãn viral PER-SOURCE + one-hot source
        │                                   + role features (rhetorical)
        ▼  train/train_viral.py
  content_model (TF-IDF→content_score)  ─┐
  + feature cấu trúc + src_* + role_*    ─┴─►  XGBoost fusion  →  P(viral)
        │
        ▼  serve/explain_viral.py
  SHAP per-prediction → JSON {viral_score, label, confidence, top_factors, explanation_text, suggestions}
```

## Thiết kế chính

- **Feature nội dung THỐNG NHẤT** cho cả 3 nguồn (hàm thuần của text): `cognitive_friction` + `char/word/has_question/is_vietnamese`.
- **Nhãn `viral` tính RIÊNG theo nguồn** (z-score `log1p` engagement của chính nền tảng đó → top `--quantile`, mặc định 0.75). Cột engagement chỉ tạo nhãn, **không** làm feature (tránh leakage).
- **`content_score`** = TF-IDF + LogReg trên text, fuse như 1 feature (tạo bằng out-of-fold để không leakage). Interface `.predict_proba(list[str])` → sau này thay **BERT** không phải sửa chỗ khác.
- **`role_*`** = vai trò rhetorical marketing (cta/hook/proof/…) từ nhánh `feature/annotation-roles-marketing`; chủ yếu phục vụ **giải thích**.
- **Giải thích** = SHAP (`pred_contribs` của XGBoost) → map feature → lý do tiếng Việt + gợi ý.

## Chạy (Python trong `ml/.venv`)

```powershell
$env:PYTHONIOENCODING='utf-8'            # Windows: tránh lỗi in tiếng Việt
# 1) role classifier (cần silver_dataset.jsonl trong ml/data/)
& ".\ml\.venv\Scripts\python.exe" ml/train/train_roles.py
# 2) dựng dataset train (gọi role model)
& ".\ml\.venv\Scripts\python.exe" ml/preprocess/build_dataset.py
# 3) train model viral + SHAP importance
& ".\ml\.venv\Scripts\python.exe" ml/train/train_viral.py
# 4) giải thích 1 bài
& ".\ml\.venv\Scripts\python.exe" ml/serve/explain_viral.py
```

Dùng trong code: `from serve.explain_viral import explain_post; explain_post(text, source)`.

## Cấu trúc file

| File | Vai trò |
|---|---|
| `preprocess/build_dataset.py` | thô → dataset train (clean, feature, nhãn per-source) |
| `features/cognitive_friction.py` | feature độ khó đọc (EN+VI) |
| `features/text_content.py` | content model TF-IDF+LogReg (`.predict_proba`) |
| `features/rhetorical_roles.py` | segment → role features per-post |
| `train/train_roles.py` | role classifier từ silver |
| `train/train_viral.py` | XGBoost fusion + đánh giá + lưu model |
| `serve/explain_viral.py` | dự đoán + SHAP → JSON giải thích |
| `models/*.joblib`, `data/*` | artifact (gitignore, không commit) |

## Kết quả hiện tại (baseline)

- Viral model: **PR-AUC ~0.48 / ROC-AUC ~0.74** (content_score là tín hiệu mạnh nhất).
- Role classifier: **macro-F1 ~0.80** trên 6 vai trò (cta/hook/proof/social_proof/pain_point/urgency).

## Giới hạn & hướng phát triển

- Content model là **TF-IDF** → tiếng Việt còn yếu; nâng **BERT đa ngữ** (train Kaggle GPU, giữ interface).
- Role: nhãn heuristic, mới 6/12 vai trò, chưa có **gold người-kiểm** để đánh giá khái quát.
- Chưa có **feature kênh/tác giả** (subscriber/follower) — cần crawl thêm; tầng "fresh/retrieve" có TTL.
- Crawl thêm dữ liệu → cân bằng hơn, nâng `--quantile` 0.75 → 0.90 theo chuẩn paper.

> Ghi chú dev chi tiết (không commit): `ml/DEV_LOG.md`.
