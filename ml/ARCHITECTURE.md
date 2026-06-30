# Kiến trúc phần AI — Dự đoán & giải thích viral (Stage 1)

> Xem đẹp nhất bằng Mermaid preview (VS Code: chuột phải → *Open Preview*, hoặc trên GitHub).

## 1. Toàn cảnh pipeline

```mermaid
flowchart TB
  subgraph IN["① Dữ liệu vào"]
    RAW["filtered_events.csv<br/>export từ Silver lakehouse<br/>YouTube · X · Reddit"]
    ANN["Nhánh annotation<br/>silver_dataset.jsonl"]
  end

  subgraph PRE["② Tiền xử lý — build_dataset.py"]
    CLEAN["clean_text<br/>bỏ URL / redaction / @ / #"]
    FILT["lọc text ngắn + bỏ trùng"]
    TXT["Feature nội dung (thống nhất)<br/>cognitive_friction · char/word<br/>has_question · is_vietnamese"]
    ROLE["Role features<br/>role_n_* · role_ratio_* · diversity"]
    SRC["one-hot nguồn<br/>src_youtube / x / reddit"]
    LAB["Nhãn viral PER-SOURCE<br/>z-score log1p engagement → top 25%"]
  end

  subgraph SUB["③ Mô hình phụ (text → tín hiệu)"]
    CM["Content model<br/>TF-IDF + LogReg<br/>→ content_score"]
    RCLS["Role classifier<br/>TF-IDF + LogReg<br/>(train từ silver)"]
  end

  subgraph FUS["④ Fusion — train_viral.py"]
    XGB["XGBoost<br/>split theo author chống leakage<br/>scale_pos_weight"]
  end

  subgraph OUT["⑤ Phục vụ — explain_viral.py"]
    PRED["P viral"]
    SHAP["SHAP pred_contribs<br/>→ top yếu tố ±"]
    JSON["JSON kết quả:<br/>viral_score · label · confidence<br/>top_factors · explanation_text · suggestions"]
  end

  RAW --> CLEAN --> FILT
  FILT --> TXT & ROLE & SRC & LAB
  ANN --> RCLS
  RCLS -.->|gán role cho từng đoạn| ROLE
  CLEAN --> CM

  TXT --> XGB
  ROLE --> XGB
  SRC --> XGB
  CM -->|content_score OOF| XGB
  LAB -.->|nhãn target| XGB

  XGB --> BUNDLE[("stage1_multisource.joblib<br/>model + content_model + features")]
  BUNDLE --> PRED --> SHAP --> JSON
  BUNDLE --> EVAL["evaluate.py<br/>PR-AUC theo từng nguồn"]
```

## 2. Feature đưa vào XGBoost (và vai trò)

```mermaid
flowchart LR
  A["content_score<br/>★ tín hiệu MẠNH nhất"]:::strong
  B["10 feature văn bản<br/>độ dài · độ khó đọc · có '?'…"]
  C["role_* — vai trò marketing<br/>(chủ yếu để GIẢI THÍCH)"]:::weak
  D["src_* — nền tảng"]
  A --> X["XGBoost → P(viral)"]
  B --> X
  C --> X
  D --> X
  classDef strong fill:#1e88e5,color:#fff;
  classDef weak fill:#9e9e9e,color:#fff;
```

## 3. Quan trọng: train vs serve (chống leakage)

```mermaid
flowchart LR
  subgraph TR["Lúc TRAIN"]
    T1["content_score = OUT-OF-FOLD<br/>(cross_val_predict)"]
    T2["nhãn viral từ engagement<br/>→ KHÔNG dùng engagement làm feature"]
  end
  subgraph SE["Lúc SERVE"]
    S1["chỉ cần TEXT + nguồn"]
    S2["content/role/structural tính lại y hệt"]
  end
```

## 4. Hiện trạng (số liệu thật)

| Thành phần | Kết quả |
|---|---|
| Fusion viral (overall) | PR-AUC ~0.48 · ROC ~0.74 |
| └ YouTube | ROC **0.80** (tốt — nhiều data) |
| └ Reddit | ROC 0.62 |
| └ X | ROC **0.49** (≈ ngẫu nhiên — quá ít data) |
| Role classifier | macro-F1 ~0.80 (6/12 vai trò) |
| Content: TF-IDF vs BERT | TF-IDF 0.499 **>** BERT 0.428 (data còn nhỏ) → giữ TF-IDF |

**Chú giải:** nét liền = luồng dữ liệu/feature; nét đứt = quan hệ phụ trợ (nhãn, gán role). Mô hình hiện mạnh ở YouTube, yếu ở X → đòn bẩy chính = **crawl thêm data X/Reddit**.

> Chi tiết kỹ thuật & lịch sử quyết định: `ml/DEV_LOG.md`. Tổng quan code: `ml/README.md`.
