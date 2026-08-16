# Pre-publication reputation contract

## Current policy

The official model excludes audience and reputation features. Existing subscriber and
follower counts were captured during scraping, not from a state proven to precede each
post. Since virality can itself increase those counts, using them would introduce
look-ahead bias. Historical audience-enabled measurements remain diagnostic only.

The active manifest policy is:

```text
audience_feature_policy = excluded_no_prepublication_history
```

Gold writes `audience_count = NULL` and `audience_available = false`, and official
training excludes every `chan_*` column even if a malformed export happens to contain
one.

## Future historical table

Reputation should be collected as append-only Iceberg observations rather than an
overwritten current value:

```text
lakehouse.silver.author_reputation_history
- source
- author_hash
- reputation_observed_at
- subscriber_count
- follower_count
- reddit_author_karma
- account_age_days
- provenance_json
- snapshot_date
```

For each post, the feature builder must select the most recent observation satisfying:

```text
reputation_observed_at <= post_published_at < label_observed_at
```

Conceptually, this is an as-of join:

```sql
SELECT post.*, reputation.*
FROM post_features AS post
ASOF LEFT JOIN author_reputation_history AS reputation
  ON post.source = reputation.source
 AND post.author_hash = reputation.author_hash
 AND reputation.reputation_observed_at <= post.event_ts;
```

Rows with no preceding observation keep reputation features null. The builder must never
substitute the nearest later observation.

## Required validation

Before enabling reputation in an official manifest, automated checks must verify:

- zero rows where `reputation_observed_at > post_published_at`;
- one deterministic observation per post after the as-of join;
- observation age and missing-rate distributions recorded in the manifest;
- the reputation-history Iceberg snapshot ID included in dataset identity;
- an ablation comparing no reputation against historically frozen reputation;
- no current-state cache or post-publication backfill enters official training.

Only after these checks pass should the manifest policy change to a new, versioned
pre-publication policy and `chan_*` features become eligible again.
