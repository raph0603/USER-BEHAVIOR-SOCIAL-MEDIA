# Marketing role annotation quality report

Date: 2026-06-26

## Method

The final 6,876 retained segments were evaluated with a deterministic
three-angle heuristic:

1. an explicit-action lexicon for calls to action, links, quantities,
   evidence, authority, urgency, and scarcity;
2. structural signals such as segment position, imperative openings, and
   syntax;
3. the previously assigned baseline label.

Confidence changes follow fixed rules:

- both independent checks confirm the baseline: add 0.12, capped at 0.95;
- one check confirms the baseline: add 0.04 to 0.07, depending on whether the
  class is directly observable or interpretive;
- neither check confirms the baseline: preserve the original confidence;
- an `uncertain` baseline becomes a concrete role only when both checks agree,
  with confidence between 0.60 and 0.85 according to signal strength.

No human review was performed. Agreement values describe internal heuristic
corroboration and must not be reported as human inter-reviewer agreement.

| Corroboration level | Segments | Share |
|---|---:|---:|
| Baseline confirmed by both checks | 528 | 7.7% |
| Baseline confirmed by one check | 776 | 11.3% |
| Baseline retained without confirmation | 1,956 | 28.4% |
| `uncertain` confirmed by both checks | 3,468 | 50.4% |
| `uncertain` changed after both checks agreed | 148 | 2.2% |

## Silver, Gold, and uncertain split

The corrected convention keeps Gold small and strict, Silver broad enough for
training, and unsupported rows separate:

| Dataset | Rule | Rows |
|---|---|---:|
| `gold_dataset.jsonl` | Confidence at least 0.95, three-way agreement, non-`uncertain` | 286 |
| `silver_dataset.jsonl` | Confidence below 0.95, non-`uncertain` | 3,122 |
| `uncertain_dataset.jsonl` | `primary_role = uncertain` | 3,468 |

Gold is below the original 500-row target because the 0.95 threshold is
deliberately strict. Lowering it to 0.90 would produce 959 rows but would also
include two-way agreement. The project keeps the stricter definition.

## Corpus statistics

- Posts after the EV filter: 955.
- Retained segments: 6,876, or 7.2 segments per post.
- Rejected noise segments: 2,796 in `segments_rejetes.jsonl`.
- Cleaned pre-filter corpus: 11,710 segments in
  `segments_a_annoter_clean.jsonl`.
- Platform distribution: YouTube 4,718; Reddit 1,124; X 1,034.
- Language distribution: English 4,695; Vietnamese 2,181.
- `uncertain`: 3,468 segments, or 50.4%.

| Primary role | Segments | Mean confidence |
|---|---:|---:|
| uncertain | 3,468 | 0.55 |
| cta | 954 | 0.85 |
| hook | 835 | 0.60 |
| educational | 367 | 0.42 |
| proof | 325 | 0.74 |
| benefit | 249 | 0.51 |
| pain_point | 213 | 0.48 |
| storytelling | 132 | 0.44 |
| objection_handling | 130 | 0.49 |
| social_proof | 87 | 0.64 |
| urgency | 77 | 0.68 |
| solution | 27 | 0.38 |
| scarcity | 12 | 0.65 |

## Limitations

- The labels have no independent human ground truth.
- The lexicons are substantially smaller for Vietnamese than for English.
- Half the source corpus contains no supported marketing role.
- Low-confidence lexical estimates remain low confidence; corroboration is not
  used to inflate weak evidence.
- The datasets fall below the original volume targets because the retained EV
  corpus is small after quality filtering.

Use Silver for model fitting, Gold for a strict heuristic reference, and keep
the uncertain split out of standard role-classifier training.
