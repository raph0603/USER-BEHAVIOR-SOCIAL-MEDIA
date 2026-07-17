# Marketing annotation datasets

This directory contains rhetorical-role and sentiment datasets for electric
vehicle posts and comments from Reddit, X, and YouTube in English and
Vietnamese.

## Read this before using the data

The role labels were produced with deterministic multilingual lexical and
structural rules. They have not been validated by human reviewers. The
three-way corroboration score is useful for ranking confidence, but it is not
equivalent to inter-reviewer agreement.

Half of the retained segments are intentionally classified as `uncertain`:
3,468 of 6,876 rows, or 50.4%. This reflects timestamps, channel boilerplate,
legal notices, and unrelated fragments in the source material. Exclude those
rows when training a rhetorical-role classifier unless `uncertain` is an
explicit target class.

The current split convention is:

| Dataset | Definition | Rows | Recommended use |
|---|---|---:|---|
| `gold_dataset.jsonl` | Confidence at least 0.95, three-way agreement, non-`uncertain` | 286 | Strict evaluation reference |
| `silver_dataset.jsonl` | Remaining non-`uncertain` segments | 3,122 | Main training set |
| `uncertain_dataset.jsonl` | No sufficiently supported marketing role | 3,468 | Error analysis or explicit reject class |

The Gold name means high heuristic confidence; it does not mean that a human
reviewer verified the rows.

## File map

- `all_posts_raw.jsonl` contains the normalized posts before balancing.
- `posts_originaux_selection.jsonl` contains the balanced post sample.
- `segments_a_annoter.jsonl` contains the initial sentence-level segments.
- `segments_a_annoter_clean.jsonl` contains the cleaned pre-filter corpus.
- `segments_rejetes.jsonl` contains rejected noise.
- `silver_dataset.jsonl`, `gold_dataset.jsonl`, and
  `uncertain_dataset.jsonl` contain the role-label splits.
- `*_sentiment.jsonl` contains role rows enriched with sentiment fields.
- `annotation_instructions.txt` defines the role taxonomy and output schema.
- `sentiment/` contains the English and Vietnamese sentiment engines and
  batch scripts.
- `rapport_preparation_dataset.md`, `rapport_qualite_annotation.md`, and
  `rapport_sentiment.md` record preparation, quality, and sentiment results.

These JSONL files are retained deliberately. Preparation scripts read the raw
and selected-post files, and `ml/train/train_roles.py` reads
`silver_dataset.jsonl`. They are reproducibility inputs, not disposable local
output.

## Known language-tag limitation

Forty-six segments originally tagged as English contained Vietnamese
diacritics. Their language tags were corrected. The available voting code did
not reproduce the original role on 22 of those 46 rows, so the existing
`primary_role` values were preserved to avoid an unverified relabeling. Rebuild
the complete corpus before changing those role values.
