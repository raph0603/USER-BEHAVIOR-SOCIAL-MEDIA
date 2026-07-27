"""The audience enrichment must not turn "we could not find out" into "no audience".

`build_dataset.add_channel_features` treats a `0` as an author who genuinely has no
followers, and the fusion model has never seen that value for a real YouTube channel. So
every path that cannot resolve a count -- a hidden subscriber count, a deleted channel, a
malformed payload -- has to yield `None`.

The HTTP call is injected, so these run offline and need no API key.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = ROOT / "ml"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENRICH = _load("ml_enrich_subs", ML_ROOT / "preprocess" / "enrich_youtube_subscribers.py")


class SubscriberParsingTests(unittest.TestCase):
    def test_a_real_count_is_read(self):
        self.assertEqual(ENRICH.subscriber_from_statistics({"subscriberCount": "984000"}), 984000)

    def test_a_hidden_count_is_unknown_not_zero(self):
        stats = {"hiddenSubscriberCount": True, "subscriberCount": "0"}

        self.assertIsNone(ENRICH.subscriber_from_statistics(stats))

    def test_a_missing_count_is_unknown(self):
        self.assertIsNone(ENRICH.subscriber_from_statistics({"viewCount": "12"}))

    def test_a_malformed_count_is_unknown(self):
        self.assertIsNone(ENRICH.subscriber_from_statistics({"subscriberCount": "many"}))

    def test_a_genuine_zero_survives(self):
        # A brand-new channel really can have zero subscribers; only hidden means unknown.
        self.assertEqual(ENRICH.subscriber_from_statistics({"subscriberCount": "0"}), 0)


class ApiBatchingTests(unittest.TestCase):
    def setUp(self):
        self.calls = []

    def _fetch(self, payload_by_id):
        def fetch(url):
            self.calls.append(url)
            ids = url.split("id=")[1].split("&")[0].split("%2C")
            return {
                "items": [
                    {"id": i, "statistics": payload_by_id[i]} for i in ids if i in payload_by_id
                ]
            }
        return fetch

    def test_requests_are_batched_at_fifty(self):
        channels = [f"UC{i:03d}" for i in range(120)]
        payload = {c: {"subscriberCount": "10"} for c in channels}

        resolved = ENRICH.fetch_via_api(channels, "KEY", fetch_json=self._fetch(payload))

        self.assertEqual(len(self.calls), 3)  # 50 + 50 + 20
        self.assertEqual(len(resolved), 120)

    def test_a_channel_the_api_omits_is_recorded_as_unknown(self):
        # Deleted or private channels come back missing; caching them as unknown stops every
        # later run from paying quota to ask again.
        channels = ["UC_ok", "UC_gone"]
        payload = {"UC_ok": {"subscriberCount": "500"}}

        resolved = ENRICH.fetch_via_api(channels, "KEY", fetch_json=self._fetch(payload))

        self.assertEqual(resolved["UC_ok"], 500)
        self.assertIn("UC_gone", resolved)
        self.assertIsNone(resolved["UC_gone"])

    def test_the_key_is_sent_and_only_statistics_are_requested(self):
        ENRICH.fetch_via_api(["UC_a"], "SECRET", fetch_json=self._fetch({"UC_a": {}}))

        self.assertIn("key=SECRET", self.calls[0])
        self.assertIn("part=statistics", self.calls[0])


if __name__ == "__main__":
    unittest.main()
