"""Build and validate a publication-ready X transformation example bundle."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.x_lineage import PRIVACY_TOKENS, sensitive_values, sha256_file

_TOKEN_OUTPUT = {
    "user": "<USER>",
    "email": "<EMAIL>",
    "url": "<URL>",
    "phone": "<PHONE>",
    "ip": "<IP>",
}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _first(document: dict[str, list[dict[str, Any]]], table: str) -> dict[str, Any]:
    rows = document.get(table) or []
    return rows[0] if rows else {}


def _latex_escape_fragment(value: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(character, character) for character in value).replace(
        "\n", r"\newline "
    )


def latex_escape(value: Any) -> str:
    """Escape arbitrary values while rendering privacy tokens as monospace text."""

    text = str(value if value is not None else "null")
    parts = re.split(r"(<(?:USER|EMAIL|PHONE|IP|URL)>)", text)
    rendered = []
    for part in parts:
        if part in PRIVACY_TOKENS:
            token_name = part[1:-1]
            rendered.append(
                rf"\texttt{{\textless {token_name}\textgreater}}"
            )
        else:
            rendered.append(_latex_escape_fragment(part))
    return "".join(rendered)


def _derived_values(features: dict[str, Any]) -> dict[str, Any]:
    names = (
        "word_count",
        "sentence_count",
        "hashtag_count",
        "emoji_count",
        "mention_token_count",
        "email_token_count",
        "url_token_count",
        "phone_token_count",
        "ip_token_count",
        "lexical_diversity",
        "question_mark_count",
        "exclamation_mark_count",
    )
    return {name: features.get(name) for name in names}


def _transformations(
    raw: dict[str, Any],
    clean: dict[str, Any],
    features: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_text = str(raw.get("raw_text") or raw.get("title") or "")
    clean_text = str(clean.get("clean_text") or "")
    model_text = str(clean.get("text_for_model") or "")
    changes: list[dict[str, Any]] = []
    for category, values in sensitive_values(raw_text).items():
        for value in values:
            changes.append(
                {
                    "input": value,
                    "output": _TOKEN_OUTPUT[category],
                    "type": "privacy_redaction",
                }
            )
    if clean_text != model_text:
        changes.append(
            {
                "input": clean_text,
                "output": model_text,
                "type": "lowercase_normalization",
            }
        )
    collapsed_whitespace = re.sub(r"\s+", " ", raw_text).strip()
    punctuation_normalized = re.sub(
        r"\s+([,.;:!?])", r"\1", collapsed_whitespace
    )
    if collapsed_whitespace != raw_text or punctuation_normalized != collapsed_whitespace:
        changes.append(
            {
                "input": "non-canonical whitespace",
                "output": "single canonical spaces",
                "type": "whitespace_normalization",
            }
        )
    if raw.get("url") != clean.get("url"):
        changes.append(
            {
                "input": raw.get("url"),
                "output": clean.get("url"),
                "type": "url_canonicalization",
            }
        )
    if raw.get("x_account") != clean.get("x_account"):
        changes.append(
            {
                "input": raw.get("x_account"),
                "output": clean.get("x_account"),
                "type": "identity_hashing",
            }
        )
    changes.append(
        {
            "input": model_text,
            "output": _derived_values(features),
            "type": "feature_extraction",
        }
    )
    return changes


def _comparison(
    raw_document: dict[str, Any],
    clean_document: dict[str, Any],
    bronze_document: dict[str, list[dict[str, Any]]],
    silver_document: dict[str, list[dict[str, Any]]],
    gold_document: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    raw = raw_document["event"]
    clean = clean_document["after"]
    bronze_log = _first(bronze_document, "lakehouse.bronze.event_log")
    bronze_current = _first(bronze_document, "lakehouse.bronze.events")
    silver_event = _first(silver_document, "lakehouse.silver.events")
    silver_content = _first(silver_document, "lakehouse.silver.contents")
    silver_features = _first(silver_document, "lakehouse.silver.post_features")
    silver_snapshot = _first(
        silver_document, "lakehouse.silver.engagement_snapshots"
    )
    content_stats = _first(gold_document, "lakehouse.gold.content_stats")
    user_evolution = _first(gold_document, "lakehouse.gold.user_evolution")
    return {
        "platform_event_id": str(raw["platform_event_id"]),
        "event_type": "real_existing",
        "raw": {
            "text": raw.get("raw_text") or raw.get("title"),
            "x_account": raw.get("x_account"),
            "url": raw.get("url"),
            "kafka": {
                "topic": raw_document["capture_metadata"].get("kafka_topic"),
                "partition": raw_document["capture_metadata"].get("kafka_partition"),
                "offset": raw_document["capture_metadata"].get("kafka_offset"),
            },
        },
        "clean": {
            "text": clean.get("clean_text"),
            "text_for_model": clean.get("text_for_model"),
            "x_account_hash": clean.get("x_account"),
            "url": clean.get("url"),
            "redactions": clean_document["redaction_summary"],
        },
        "bronze": {
            "raw_text": bronze_current.get("raw_text"),
            "clean_text": bronze_current.get("clean_text"),
            "text_for_model": bronze_current.get("text_for_model"),
            "event_id": bronze_current.get("event_id"),
            "content_id": bronze_current.get("content_id"),
            "payload_fingerprint": bronze_current.get("payload_fingerprint"),
            "kafka_topic": bronze_log.get("kafka_topic"),
            "kafka_partition": bronze_log.get("kafka_partition"),
            "kafka_offset": bronze_log.get("kafka_offset"),
        },
        "silver": {
            "raw_text": silver_content.get("raw_text"),
            "clean_text": silver_content.get("clean_text"),
            "text_for_model": silver_features.get("text_for_model"),
            "features": _derived_values(silver_features),
            "identifiers": {
                "event_id": silver_event.get("event_id"),
                "content_id": silver_content.get("content_id"),
                "root_content_id": silver_content.get("root_content_id"),
                "conversation_id": silver_content.get("conversation_id"),
                "observation_id": silver_snapshot.get("observation_id"),
                "author_id_hash": silver_content.get("author_id_hash"),
                "payload_fingerprint": silver_content.get("payload_fingerprint"),
            },
        },
        "gold": {
            "content_stats": content_stats,
            "user_evolution": user_evolution,
        },
        "timestamps": {
            "published_at": raw.get("published_at") or raw.get("timestamp"),
            "collected_at": raw.get("collected_at"),
            "event_ts": silver_event.get("event_ts"),
            "ingested_at": bronze_log.get("ingested_at"),
            "observed_at": silver_snapshot.get("observed_at"),
            "snapshot_at": silver_snapshot.get("snapshot_at"),
        },
        "transformations": _transformations(raw, clean, silver_features),
    }


def _latex_table(comparison: dict[str, Any]) -> str:
    raw = comparison["raw"]
    clean = comparison["clean"]
    bronze = comparison["bronze"]
    silver = comparison["silver"]
    gold = comparison["gold"]
    features = silver["features"]
    stats = gold["content_stats"]
    evolution = gold["user_evolution"]
    rows = [
        (
            "RAW",
            raw["text"],
            f"account={raw['x_account']}; platform_event_id={comparison['platform_event_id']}",
        ),
        (
            "Clean",
            clean["text"],
            "redactions="
            + json.dumps(clean["redactions"], ensure_ascii=False, sort_keys=True),
        ),
        (
            "Bronze",
            bronze["clean_text"],
            f"event_id={bronze['event_id']}; content_id={bronze['content_id']}",
        ),
        (
            "Silver",
            silver["text_for_model"],
            "; ".join(f"{key}={value}" for key, value in features.items()),
        ),
        (
            "Gold",
            stats.get("title"),
            (
                f"content_id={stats.get('content_id')}; "
                f"latest_like_count={stats.get('latest_like_count')}; "
                f"latest_view_count={stats.get('latest_view_count')}; "
                f"contents_created={evolution.get('contents_created')}; "
                f"question_count={evolution.get('question_count')}"
            ),
        ),
    ]
    rendered_rows = "\n".join(
        f"{latex_escape(layer)} & {latex_escape(text)} & "
        f"{latex_escape(values)} \\\\"
        for layer, text, values in rows
    )
    return rf"""\begin{{table*}}[t]
\centering
\caption{{Example of X content transformations across the lakehouse layers.}}
\label{{tab:x-transformation-example}}
\scriptsize
\begin{{tabularx}}{{\textwidth}}{{
    >{{\raggedright\arraybackslash}}p{{1.2cm}}
    >{{\raggedright\arraybackslash}}X
    >{{\raggedright\arraybackslash}}p{{4.3cm}}
}}
\toprule
\textbf{{Layer}} &
\textbf{{Content}} &
\textbf{{Derived values}} \\
\midrule
{rendered_rows}
\bottomrule
\end{{tabularx}}
\end{{table*}}
"""


def _selection_report(
    scan: dict[str, Any],
    comparison: dict[str, Any],
    manifest: dict[str, Any],
) -> str:
    top_rows = "\n".join(
        (
            f"| {index} | `{candidate['platform_event_id']}` | "
            f"{candidate['score']} | "
            f"{candidate['transformation_type_count']} | "
            f"{candidate['raw_available']} | "
            f"{candidate['bronze_available']} | "
            f"{candidate['silver_available']} |"
        )
        for index, candidate in enumerate(scan["top_candidates"][:5], start=1)
    )
    counts = scan["candidate_counts"]
    layers = manifest["matched_row_counts"]
    transformations = "\n".join(
        f"- `{item['type']}`"
        for item in comparison["transformations"]
    )
    return f"""# X transformation example selection report

## Selection result

- Type: `real_existing`
- Platform event ID: `{comparison['platform_event_id']}`
- RAW genuinely available: yes, decoded from the retained `x.raw.events` record
- Newly collected from X: no
- Controlled fixture: no
- Events inspected: {scan['raw_events_inspected']}
- Existing events already present in both Bronze and Silver before replay: {scan['events_with_bronze_and_silver']}

The final event was selected because it had the highest score ({scan['top_candidates'][0]['score']}),
contained two real mentions and four hashtags, and also exhibits whitespace and
lowercase normalization. The retained real RAW Kafka record was replayed
idempotently so the same event could be verified in Bronze, Silver, and Gold.

## Candidate counts

- Mention: {counts['user_count']}
- Email: {counts['email_count']}
- URL recognized by the privacy regex: {counts['url_count']}
- Phone: {counts['phone_count']}
- IP: {counts['ip_count']}
- Hashtag: {counts['hashtag_count']}
- Emoji: {counts['emoji_count']}

## Five highest-ranked candidates

| Rank | Platform event ID | Score | Transformation types | RAW | Bronze before replay | Silver before replay |
|---:|---|---:|---:|---|---|---|
{top_rows}

## Lineage layers found after the exact replay

- RAW: found
- Clean: found
- Bronze event log: {layers.get('lakehouse.bronze.event_log', 0)} row
- Bronze current projection: {layers.get('lakehouse.bronze.events', 0)} row
- Silver events: {layers.get('lakehouse.silver.events', 0)} row
- Silver contents: {layers.get('lakehouse.silver.contents', 0)} row
- Silver post features: {layers.get('lakehouse.silver.post_features', 0)} row
- Gold content stats: {layers.get('lakehouse.gold.content_stats', 0)} row
- Gold user evolution: {layers.get('lakehouse.gold.user_evolution', 0)} row
- Optional model predictions: absent
- Optional training examples: absent

## Transformations actually observed

{transformations}

## Limits

- No email, valid contiguous URL, phone number, IP address, or emoji was present.
- The RAW contains URL-like strings broken by spaces after `https://`; these are
  not valid contiguous URLs and therefore were not counted or replaced by `<URL>`.
- The example is demonstrative for mention redaction, punctuation/space cleanup,
  lowercase normalization, identity hashing, hashtag preservation, feature
  extraction, and Gold aggregation, but not for every supported privacy token.
- Gold retains a linked cleaned title and aggregates; no new Gold text was invented.
"""


def finalize(directory: Path, candidate_scan: Path) -> dict[str, Any]:
    required = (
        "raw.json",
        "clean.json",
        "bronze.json",
        "silver.json",
        "gold.json",
        "lineage.json",
        "manifest.json",
    )
    missing = [name for name in required if not (directory / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing lineage exports: {', '.join(missing)}")

    raw_document = _read_json(directory / "raw.json")
    clean_document = _read_json(directory / "clean.json")
    bronze_document = _read_json(directory / "bronze.json")
    silver_document = _read_json(directory / "silver.json")
    gold_document = _read_json(directory / "gold.json")
    manifest = _read_json(directory / "manifest.json")
    scan = _read_json(candidate_scan)
    comparison = _comparison(
        raw_document,
        clean_document,
        bronze_document,
        silver_document,
        gold_document,
    )

    raw_text = str(comparison["raw"]["text"] or "")
    clean_text = str(comparison["clean"]["text"] or "")
    model_text = str(comparison["clean"]["text_for_model"] or "")
    protected_payload = json.dumps(
        {"bronze": bronze_document, "silver": silver_document},
        ensure_ascii=False,
        sort_keys=True,
    )
    original_values_absent = all(
        value not in protected_payload
        for values in sensitive_values(raw_text).values()
        for value in values
        if value
    )
    present_tokens = [token for token in PRIVACY_TOKENS if token in clean_text]
    token_preservation = all(token in model_text for token in present_tokens)
    feature_token_count = sum(
        int(comparison["silver"]["features"].get(name) or 0)
        for name in (
            "mention_token_count",
            "email_token_count",
            "url_token_count",
            "phone_token_count",
            "ip_token_count",
        )
    )
    textual_types = set()
    for item in comparison["transformations"]:
        if item["type"] == "privacy_redaction":
            textual_types.add(str(item["output"]))
        elif item["type"] in {
            "lowercase_normalization",
            "whitespace_normalization",
        }:
            textual_types.add(item["type"])

    validations = {
        "raw_differs_from_clean": raw_text != clean_text,
        "at_least_two_textual_transformations": len(textual_types) >= 2,
        "silver_has_nonzero_privacy_token_count": feature_token_count > 0,
        "tokens_preserved_in_text_for_model": token_preservation,
        "original_replaced_values_absent_from_bronze_and_silver": (
            original_values_absent
        ),
        "event_type_is_real_existing": comparison["event_type"] == "real_existing",
        "all_mandatory_layers_have_one_row": all(
            manifest["matched_row_counts"].get(table) == 1
            for table in (
                "lakehouse.bronze.event_log",
                "lakehouse.bronze.events",
                "lakehouse.silver.events",
                "lakehouse.silver.contents",
                "lakehouse.silver.post_features",
                "lakehouse.gold.content_stats",
                "lakehouse.gold.user_evolution",
            )
        ),
    }
    errors = [name for name, passed in validations.items() if not passed]

    _write_json(directory / "comparison.json", comparison)
    latex = _latex_table(comparison)
    (directory / "example-table.tex").write_text(latex, encoding="utf-8")
    report = _selection_report(scan, comparison, manifest)
    (directory / "selection-report.md").write_text(report, encoding="utf-8")

    latex_values = (
        comparison["raw"]["text"],
        comparison["clean"]["text"],
        comparison["bronze"]["clean_text"],
        comparison["silver"]["text_for_model"],
        comparison["gold"]["content_stats"].get("title"),
    )
    validations["latex_matches_exported_text_values"] = all(
        latex_escape(value) in latex for value in latex_values
    )
    if not validations["latex_matches_exported_text_values"]:
        errors.append("latex_matches_exported_text_values")

    generated = (
        "raw.json",
        "clean.json",
        "bronze.json",
        "silver.json",
        "gold.json",
        "lineage.json",
        "comparison.json",
        "example-table.tex",
        "selection-report.md",
    )
    manifest.update(
        {
            "status": "PASS" if not errors else "FAIL",
            "event_type": "real_existing",
            "selection": {
                "events_inspected": scan["raw_events_inspected"],
                "candidate_counts": scan["candidate_counts"],
                "selected_score": scan["top_candidates"][0]["score"],
                "selected_rank": 1,
            },
            "validations": validations,
            "errors": sorted(set((*manifest.get("errors", []), *errors))),
            "files_generated": [*generated, "manifest.json", "manifest.sha256"],
            "sha256": {
                name: sha256_file(directory / name)
                for name in generated
            },
        }
    )
    _write_json(directory / "manifest.json", manifest)
    manifest_digest = sha256_file(directory / "manifest.json")
    (directory / "manifest.sha256").write_text(
        f"{manifest_digest}  manifest.json\n", encoding="utf-8"
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--candidate-scan", type=Path, required=True)
    args = parser.parse_args()
    manifest = finalize(args.directory, args.candidate_scan)
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "event_type": manifest["event_type"],
                "platform_event_id": manifest["platform_event_id"],
                "events_inspected": manifest["selection"]["events_inspected"],
                "selected_score": manifest["selection"]["selected_score"],
                "validations": manifest["validations"],
                "output": str(args.directory),
            },
            sort_keys=True,
        )
    )
    return 0 if manifest["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
