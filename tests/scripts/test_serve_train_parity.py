"""Serving must build the feature row the way training built it.

The two drifted apart once already: training switched an unknown audience to NaN while
`explain_viral` kept substituting 0, so every request that did not carry an audience was
scored as an author with no followers. The model had never seen a 0 and read it as a real,
tiny audience -- one sample dropped from 0.545 to 0.045 while reporting 91% confidence.

These tests need the trained artifacts, which are gitignored, so they skip when the model
is absent (CI) and guard the developer loop, which is where the drift happened.
"""

import sys
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
MODEL_PATH = ML_ROOT / "models" / "stage1_multisource.joblib"
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))


@unittest.skipUnless(MODEL_PATH.exists(), f"trained model not available at {MODEL_PATH}")
class AudienceParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from serve.explain_viral import ViralExplainer

        cls.explainer = ViralExplainer()
        cls.text = "This EV has insane range. Over 300,000 drivers already switched."

    def _row(self, audience):
        return self.explainer._feature_row(self.text, "x", audience)

    def test_an_unknown_audience_is_missing_not_zero(self):
        row = self._row(None)

        self.assertTrue(np.isnan(row.loc[0, "chan_log_audience"]))
        self.assertEqual(row.loc[0, "chan_has_audience"], 0.0)
        self.assertEqual(row.loc[0, "chan_audience_is_zero"], 0.0)

    def test_an_observed_zero_is_a_real_audience_of_zero(self):
        row = self._row(0)

        self.assertEqual(row.loc[0, "chan_log_audience"], 0.0)
        self.assertEqual(row.loc[0, "chan_has_audience"], 1.0)
        self.assertEqual(row.loc[0, "chan_audience_is_zero"], 1.0)

    def test_a_known_audience_is_log_scaled_like_training(self):
        row = self._row(1000)

        self.assertAlmostEqual(row.loc[0, "chan_log_audience"], float(np.log1p(1000)))
        self.assertEqual(row.loc[0, "chan_audience_is_zero"], 0.0)

    def test_reddit_ignores_community_size_as_author_audience(self):
        row = self.explainer._feature_row(self.text, "reddit", 100_000)

        self.assertTrue(np.isnan(row.loc[0, "chan_log_audience"]))
        self.assertEqual(row.loc[0, "chan_has_audience"], 0.0)
        self.assertEqual(row.loc[0, "chan_audience_available"], 0.0)
        self.assertEqual(row.loc[0, "chan_audience_is_zero"], 0.0)

    def test_every_training_feature_is_produced(self):
        row = self._row(None)

        self.assertEqual(list(row.columns), list(self.explainer.features))


@unittest.skipUnless(MODEL_PATH.exists(), f"trained model not available at {MODEL_PATH}")
class DecisionThresholdTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from serve.explain_viral import ViralExplainer

        cls.explainer = ViralExplainer()

    def test_the_label_follows_the_bundled_threshold_not_a_hardcoded_half(self):
        # Only ~25% of posts go viral, so a calibrated score rarely passes 0.5. Pinning the
        # label to 0.5 would silently turn most viral-likely posts into not-viral.
        result = self.explainer.explain("Limited offer, order today!", "x")
        expected = "viral-likely" if result["viral_score"] >= self.explainer.threshold else "not-viral"

        self.assertEqual(result["label"], expected)

    def test_the_explanation_states_the_threshold_it_used(self):
        result = self.explainer.explain("Limited offer, order today!", "x")

        self.assertIn(f"decision threshold {self.explainer.threshold:.0%}", result["explanation_text"])


if __name__ == "__main__":
    unittest.main()
