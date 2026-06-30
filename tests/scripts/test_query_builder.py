import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "dashboard" / "query_builder.py"
SPEC = importlib.util.spec_from_file_location("query_builder", MODULE_PATH)
QUERY_BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUERY_BUILDER)


class QueryBuilderTests(unittest.TestCase):
    def test_builds_platform_specific_queries(self):
        keywords = ["electric vehicle", "Tesla"]

        self.assertEqual(
            QUERY_BUILDER.build_youtube_query(keywords, "OR"),
            '"electric vehicle" | Tesla',
        )
        self.assertEqual(
            QUERY_BUILDER.build_x_query(
                keywords,
                "OR",
                "fr",
                "filter:media",
                True,
            ),
            '("electric vehicle" OR Tesla) lang:fr filter:media '
            "-filter:replies",
        )

    def test_keywords_are_appended_and_deduplicated(self):
        self.assertEqual(
            QUERY_BUILDER.normalize_items(["EV", "Tesla", "ev", ""]),
            ["EV", "Tesla"],
        )

    def test_normalizes_subreddit(self):
        self.assertEqual(
            QUERY_BUILDER.normalize_subreddit("r/electricvehicles/"),
            "electricvehicles",
        )


if __name__ == "__main__":
    unittest.main()
