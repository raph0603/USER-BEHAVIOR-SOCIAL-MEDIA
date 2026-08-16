# AI architecture — viral prediction & explanation (Stage 1)

> Best viewed with a Mermaid preview (VS Code: right-click → *Open Preview*, or on GitHub).

## 1. End-to-end pipeline

```mermaid
flowchart TB
  subgraph IN["1. Input data"]
    RAW["filtered_events.csv<br/>exported from the Silver lakehouse<br/>YouTube · X · Reddit"]
    ANN["annotation branch<br/>silver_dataset.jsonl"]
  end

  subgraph PRE["2. Preprocess — build_dataset.py"]
    CLEAN["clean_text<br/>strip URL / redaction / @ / #"]
    FILT["drop short text + duplicates"]
    TXT["Unified content features<br/>cognitive_friction · char/word<br/>has_question · is_vietnamese"]
    ROLE["Exploratory role cues<br/>role_n_* · role_ratio_* · diversity"]
    TOPIC["Topic features<br/>topic_0..7 (NMF)"]
    SRC["source one-hot<br/>src_youtube / x / reddit"]
    LAB["Versioned viral label<br/>fixed platform threshold<br/>from a frozen reference"]
  end

  subgraph SUB["3. Sub-models (text → signal)"]
    CM["Content model<br/>TF-IDF + LogReg<br/>→ content_score"]
    RCLS["Exploratory role classifier<br/>TF-IDF + LogReg<br/>automated silver; no human gold"]
    TM["Topic model<br/>NMF over TF-IDF"]
  end

  subgraph FUS["4. Fusion — train_viral.py"]
    XGB["XGBoost<br/>split by author (anti-leakage)<br/>scale_pos_weight"]
  end

  subgraph OUT["5. Serving — explain_viral.py"]
    PRED["P(viral)"]
    SHAP["SHAP pred_contribs<br/>→ top factors ±"]
    JSON["JSON result:<br/>viral_score · label · confidence<br/>top_factors · explanation_text · suggestions"]
  end

  RAW --> CLEAN --> FILT
  FILT --> TXT & ROLE & TOPIC & SRC & LAB
  ANN --> RCLS
  RCLS -.->|assign role per segment| ROLE
  TM -.->|topic distribution| TOPIC
  CLEAN --> CM

  TXT --> XGB
  ROLE --> XGB
  TOPIC --> XGB
  SRC --> XGB
  CM -->|content_score OOF| XGB
  LAB -.->|label (target)| XGB

  XGB --> BUNDLE[("stage1_multisource.joblib<br/>model + content_model + features")]
  BUNDLE --> PRED --> SHAP --> JSON
  BUNDLE --> EVAL["evaluate.py<br/>PR-AUC per source"]
```

Official runs additionally persist the dataset manifest, environment manifest, resolved
training configuration, exact split membership, experiment lineage, serialized-model
SHA-256, and evaluation artifact. See
[`docs/EXPERIMENT_REPRODUCIBILITY.md`](../docs/EXPERIMENT_REPRODUCIBILITY.md).

## 2. Features fed into XGBoost (and their role)

```mermaid
flowchart LR
  A["content_score<br/>★ strongest signal"]:::strong
  B["content features<br/>length · reading difficulty · has '?'…"]
  C["topic_* — NMF topics<br/>(clear lift)"]:::strong
  D["role_* — exploratory cues<br/>qualitative TreeSHAP interpretation"]:::weak
  E["src_* — platform"]
  A --> X["XGBoost → P(viral)"]
  B --> X
  C --> X
  D --> X
  E --> X
  classDef strong fill:#1e88e5,color:#fff;
  classDef weak fill:#9e9e9e,color:#fff;
```

## 3. Train vs serve (anti-leakage)

```mermaid
flowchart LR
  subgraph TR["At TRAIN time"]
    T1["content_score = OUT-OF-FOLD<br/>(cross_val_predict)"]
    T2["viral label from engagement<br/>→ engagement is NEVER a feature"]
  end
  subgraph SE["At SERVE time"]
    S1["only TEXT + source needed"]
    S2["content/role/topic/structural recomputed identically"]
  end
```

## 4. Current status (real numbers)

41-row author-grouped test set. Ranking metrics with 95% bootstrap CIs, on the calibrated scores that
serving actually returns (`train/verify_answers.py`).

| Component | Result |
|---|---|
| Fusion viral (overall) | PR-AUC **0.193** [0.030, 0.697] · ROC **0.623** [0.154, 0.950] |
| └ YouTube | ROC **0.200** [0.000, 0.600] (6 test rows) |
| └ X | ROC **0.803** [0.545, 0.992] (35 test rows) |
| └ Reddit | no eligible test row in this pinned version |
| Probability calibration | ECE **0.207**, Brier 0.110; decision threshold **0.26**, picked out-of-fold |
| Exploratory role classifier | macro-F1 **0.495** against held-out heuristic silver, not human gold |
| Role ablation | no demonstrated lift; PR-AUC 0.193 with roles vs 0.271 without, paired CI crosses zero |
| Content: TF-IDF vs BERT | TF-IDF 0.499 **>** BERT 0.428 (data still small) → keep TF-IDF |

**Legend:** solid arrows = data/feature flow; dashed arrows = auxiliary relations
(label, role/topic assignment). Role SHAP contributions explain model behavior only; they
are not evidence that the underlying rhetorical classification is linguistically correct.
The small pinned sample and wide intervals make additional labelled data the main priority.

> Technical details & decision history are kept in a local engineering log. Code overview: `ml/README.md`. Handoff for the API/UI tasks: `ml/HANDOFF.md`.
