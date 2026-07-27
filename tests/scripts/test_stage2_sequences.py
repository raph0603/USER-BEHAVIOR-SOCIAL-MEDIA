"""The Stage-2 sequence builder must never see the future it is asked to predict.

Features describe the first hours after publishing; the label describes what happened
much later. Everything here is synthetic because `silver.engagement_snapshots` is
append-only and still accumulating — these tests are what proves the layer correct until
real trajectories exist.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


STAGE2 = _load("ml_build_stage2", ML_ROOT / "preprocess" / "build_stage2_dataset.py")


def observation(post: str, age_hours: float, views: int, likes: int = 0, **extra) -> dict:
    row = {
        "source": "youtube",
        "platform_event_id": post,
        "url": f"https://youtu.be/{post}",
        "age_minutes": age_hours * 60,
        "view_count": views,
        "like_count": likes,
        "comment_count": 0,
        "views_per_hour": views / max(age_hours, 1e-9),
        "views_acceleration": 0.0,
    }
    row.update(extra)
    return row


def frame(rows) -> pd.DataFrame:
    return STAGE2.prepare_snapshots(pd.DataFrame(rows))


class SequenceWindowTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            observation("a", 0.5, 100),
            observation("a", 2.0, 900),
            observation("a", 6.0, 4100),
            observation("a", 30.0, 9000),  # outcome only
        ]

    def test_features_ignore_observations_after_the_horizon(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)

        self.assertEqual(len(built), 1)
        row = built.iloc[0]
        self.assertEqual(row["seq_n_observations"], 3)  # the 30h reading is excluded
        self.assertEqual(row["seq_last_age_hours"], 6.0)
        self.assertAlmostEqual(row["seq_log_view_count"], np.log1p(4100))

    def test_the_label_uses_the_late_observation_not_the_window(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)

        # view_count on the output row is the outcome counter (9000), not the window's 4100.
        self.assertEqual(built.iloc[0]["view_count"], 9000)

    def test_moving_the_horizon_changes_what_the_features_can_see(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=2, label_hours=24)

        row = built.iloc[0]
        self.assertEqual(row["seq_n_observations"], 2)
        self.assertAlmostEqual(row["seq_log_view_count"], np.log1p(900))

    def test_no_feature_column_leaks_the_outcome_counter(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)

        features = built[STAGE2.feature_columns(built)].iloc[0]
        self.assertFalse(
            any(np.isclose(v, np.log1p(9000)) for v in features if isinstance(v, float) and not np.isnan(v))
        )


class DroppedPostTests(unittest.TestCase):
    def test_a_post_without_a_late_observation_is_dropped_not_guessed(self):
        rows = [observation("b", 0.5, 100), observation("b", 3.0, 400)]

        built = STAGE2.build_sequences(frame(rows), horizon_hours=6, label_hours=24)

        self.assertTrue(built.empty)

    def test_a_single_early_observation_cannot_describe_a_trajectory(self):
        rows = [observation("c", 1.0, 100), observation("c", 30.0, 9000)]

        built = STAGE2.build_sequences(frame(rows), horizon_hours=6, label_hours=24)

        self.assertTrue(built.empty)

    def test_a_post_first_seen_after_the_horizon_is_dropped(self):
        rows = [observation("d", 10.0, 5000), observation("d", 30.0, 9000)]

        built = STAGE2.build_sequences(frame(rows), horizon_hours=6, label_hours=24)

        self.assertTrue(built.empty)

    def test_posts_are_kept_separate(self):
        rows = [
            observation("e", 0.5, 100), observation("e", 3.0, 400), observation("e", 30.0, 9000),
            observation("f", 0.5, 50), observation("f", 3.0, 80), observation("f", 30.0, 200),
        ]

        built = STAGE2.build_sequences(frame(rows), horizon_hours=6, label_hours=24)

        self.assertEqual(len(built), 2)
        self.assertEqual(set(built["view_count"]), {9000, 200})


class FusionInputTests(unittest.TestCase):
    """The trainer joins back to the Stage-1 dataset by URL; the sequences must carry it."""

    def setUp(self):
        self.rows = [
            observation("k", 0.5, 100), observation("k", 3.0, 400), observation("k", 30.0, 9000),
        ]

    def test_the_join_key_survives_aggregation(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)

        self.assertEqual(built.iloc[0]["url"], "https://youtu.be/k")

    def test_stage1_score_joins_the_feature_set_only_once_present(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)
        TRAIN2 = _load("ml_train_stage2", ML_ROOT / "train" / "train_stage2.py")

        self.assertNotIn("stage1_score", TRAIN2.feature_columns(built))
        built["stage1_score"] = 0.4
        self.assertIn("stage1_score", TRAIN2.feature_columns(built))

    def test_the_join_key_is_never_a_feature(self):
        built = STAGE2.build_sequences(frame(self.rows), horizon_hours=6, label_hours=24)

        self.assertNotIn("url", STAGE2.feature_columns(built))


class ContractTests(unittest.TestCase):
    def test_a_horizon_at_or_past_the_label_is_rejected(self):
        rows = [observation("g", 1.0, 10), observation("g", 30.0, 90)]

        with self.assertRaises(ValueError):
            STAGE2.build_sequences(frame(rows), horizon_hours=24, label_hours=24)

    def test_a_post_is_identified_by_url_when_the_platform_id_is_missing(self):
        rows = [
            observation("h", 0.5, 100), observation("h", 3.0, 400), observation("h", 30.0, 9000),
        ]
        for row in rows:
            row["platform_event_id"] = None

        built = STAGE2.build_sequences(frame(rows), horizon_hours=6, label_hours=24)

        self.assertEqual(len(built), 1)


if __name__ == "__main__":
    unittest.main()
