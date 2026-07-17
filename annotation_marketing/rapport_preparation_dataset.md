# Marketing role dataset preparation report

Date: 2026-06-24

## Objective

Build a post-balanced English and Vietnamese dataset from YouTube, X, and
Reddit, then split each post into short units for rhetorical marketing-role
labeling. The taxonomy includes `hook`, `pain_point`, `solution`, `benefit`,
`proof`, `social_proof`, `urgency`, `scarcity`, `objection_handling`, `cta`,
`educational`, `storytelling`, and `uncertain`.

## Preparation pipeline

1. `reddit_post_scraper.py` collected missing Reddit post text through an
   authenticated CDP browser and handled HTTP 429 responses with a growing
   backoff. It recovered 732 English and 211 Vietnamese posts.
2. `extract_posts.py` normalized all three sources to
   `{post_id, platform, language, text}`.
3. `build_balanced_sample.py` retained text between 20 and 3,000 characters
   and balanced each language to the least represented platform with a fixed
   random seed.
4. `segment_posts.py` split posts into sentences and separated clauses joined
   by common coordinating conjunctions when one sentence mixed two roles.

## Balanced posts

| Platform | Language | Retained posts |
|---|---|---:|
| Reddit | EN | 418 |
| X | EN | 418 |
| YouTube | EN | 418 |
| Reddit | VI | 135 |
| X | VI | 135 |
| YouTube | VI | 135 |

Total: 1,659 posts. Reddit Vietnamese data is the limiting group with 135
quality-filtered posts.

## Produced segments

| Platform | Language | Segments | Mean segments per post |
|---|---|---:|---:|
| Reddit | EN | 1,577 | 3.8 |
| X | EN | 1,408 | 3.4 |
| YouTube | EN | 7,361 | 17.6 |
| Reddit | VI | 1,319 | 9.8 |
| X | VI | 901 | 6.7 |
| YouTube | VI | 1,940 | 14.4 |

Total: 14,506 segments. Median length is 48 characters, mean length is 61.7,
and maximum length is 1,754.

## Sampling caveat

Balancing was performed at the post level, not at the segment level. Longer
YouTube descriptions produce substantially more segments; YouTube English
accounts for roughly 51% of all English segments. A segment-balanced training
set therefore needs an additional deterministic subsample or a per-post
segment cap.

## Produced artifacts

- `all_posts_raw.jsonl`: 2,798 normalized posts before balancing.
- `posts_originaux_selection.jsonl`: 1,659 balanced posts.
- `segments_a_annoter.jsonl`: 14,506 segments before the later EV and noise
  filters.
- `annotation_instructions.txt`: role definitions and expected JSON schema.

The subsequent quality pass applies the labeling specification, separates
unsupported rows, and writes the Silver, Gold, and uncertain datasets.
