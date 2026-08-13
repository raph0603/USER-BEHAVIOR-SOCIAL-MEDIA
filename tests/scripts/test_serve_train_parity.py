"""Serving must preserve the official pre-publication feature contract.

Official models exclude audience because the collected value is not guaranteed to predate
publication. The compatibility API can still accept the field, but it must not affect the
feature row or prediction until timestamped reputation history is available.

These tests need the trained artifacts, which are gitignored, so they skip when the model
is absent (CI) and guard the developer loop, which is where the drift happened.
"""

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"
MODEL_PATH = ML_ROOT / "models" / "stage1_multisource.joblib"
if str(ML_ROOT) not in sys.path:
    sys.path.insert(0, str(ML_ROOT))


@unittest.skipUnless(MODEL_PATH.exists(), f"trained model not available at {MODEL_PATH}")
class TemporalLeakageGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from serve.explain_viral import ViralExplainer

        cls.explainer = ViralExplainer()
        cls.text = "This EV has insane range. Over 300,000 drivers already switched."

    def _row(self, audience):
        return self.explainer._feature_row(self.text, "x", audience)

    def test_official_model_contains_no_audience_feature(self):
        self.assertFalse(any(name.startswith("chan_") for name in self.explainer.features))

    def test_compatibility_audience_argument_cannot_change_official_features(self):
        without_audience = self._row(None)
        observed_zero = self._row(0)
        later_audience = self._row(1000)

        pd.testing.assert_frame_equal(without_audience, observed_zero)
        pd.testing.assert_frame_equal(without_audience, later_audience)

    def test_compatibility_audience_argument_cannot_change_official_score(self):
        scores = {
            self.explainer.explain(self.text, "x", audience)["viral_score"]
            for audience in (None, 0, 1000)
        }

        self.assertEqual(len(scores), 1)

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
