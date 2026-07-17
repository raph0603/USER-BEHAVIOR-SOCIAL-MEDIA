# Sentiment inference report for posts and comments

Date: 2026-06-26

## Implementation

- `ev_lexicon_en.py` adds roughly 50 electric-vehicle terms and expressions
  to VADER, including `range_anxiety`, `battery_fire`, `tax_credit`, and
  `eco_friendly`.
- `vi_sentiment_engine.py` implements a Vietnamese polarity lexicon of roughly
  70 terms, negators, and intensifiers. Its normalized compound score uses the
  same -1 to 1 range as VADER.
- Post sentiment is adjusted for existing rhetorical roles. Reassurance under
  `objection_handling` uses a 0.45 multiplier; `urgency` and `scarcity` use
  0.70. `pain_point` rows receive an `intentional_negative_rhetoric` flag
  without changing their score.
- Comment outputs include both sentiment and stance.

| Platform | Processed rows | English | Vietnamese |
|---|---:|---:|---:|
| Reddit | 108,645 | 108,645 | 0 |
| X | 16,500 | 16,491 | 8 |
| YouTube | 621,074 | 620,236 | 838 |

The YouTube total is 621,074 records. A raw line count overstated it because
some quoted CSV comments contain embedded newlines.

## Consistency check

The English engine was compared with earlier unextended VADER outputs:

| Dataset | Positive | Neutral | Negative |
|---|---:|---:|---:|
| Reddit baseline | 50,672 | 28,746 | 29,227 |
| Reddit EV extension | 50,919 | 28,356 | 29,370 |
| X baseline | 6,801 | 6,966 | 2,732 |
| X EV extension | 6,846 | 6,907 | 2,746 |

The close distributions show that the EV extension changes a measured subset
of records without shifting the overall baseline unexpectedly. Batch
checkpoints allowed the large YouTube corpus to resume without losing
completed work.

## Limitations

- The Vietnamese lexicon is much smaller than VADER's English vocabulary and
  has not been evaluated against a statistically representative reference
  set.
- Vietnamese tokenization uses whitespace, so variations of multiword
  expressions may be missed.
- Diacritic-based language detection misclassifies informal Vietnamese text
  written without diacritics as English.
- Role multipliers are hand-set and have not been calibrated against sentiment
  ground truth.
- Stance uses a small set of explicit markers and otherwise falls back to
  general sentiment.
- Validation compares against an earlier VADER baseline; it does not provide
  precision, recall, or F1 against human-reviewed sentiment labels.

## Recommended improvements

- Expand the Vietnamese lexicon to 200-300 terms using uncovered examples from
  the real corpus.
- Add common Vietnamese function words without diacritics to language
  detection.
- Validate 100-150 mixed-language, mixed-platform comments with independent
  human review.
- Calibrate role multipliers on a controlled high-confidence sample.
- Replace the stance fallback with a dedicated rule classifier that considers
  sentence structure.

## Artifacts

- `sentiment/ev_lexicon_en.py`
- `sentiment/vi_sentiment_engine.py`
- `sentiment/sentiment_engine.py`
- `sentiment/apply_sentiment_posts.py`
- `sentiment/apply_sentiment_comments.py`
- `silver_dataset_sentiment.jsonl`
- `gold_dataset_sentiment.jsonl`
- `sentiment/output/reddit_sentiment.csv`
- `sentiment/output/x_sentiment.csv`
- `sentiment/output/youtube_sentiment.csv`

The sentiment-enriched role files retain the older partition and should be
rebuilt before they are assumed to match the corrected 3,122-row Silver and
286-row Gold split. Comment outputs join existing feature files by
`comment_id` or `status_id`.
