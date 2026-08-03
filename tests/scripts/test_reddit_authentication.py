import ast
import os
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PRODUCER_PATH = ROOT / "playwright" / "producer.py"


def _load_reddit_auth_cookies():
    tree = ast.parse(PRODUCER_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_reddit_auth_cookies"
    )
    namespace = {"os": os}
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, str(PRODUCER_PATH), "exec"), namespace)
    return namespace["_reddit_auth_cookies"]


class RedditAuthenticationTests(unittest.TestCase):
    def test_reddit_session_cookie_is_mapped_for_playwright(self):
        auth_cookies = _load_reddit_auth_cookies()

        with patch.dict(
            os.environ,
            {
                "REDDIT_SESSION_COOKIE": "session-secret",
                "REDDIT_CSRF_TOKEN_COOKIE": "csrf-secret",
            },
            clear=True,
        ):
            cookies = auth_cookies()

        self.assertEqual([cookie["name"] for cookie in cookies], ["reddit_session", "csrf_token"])
        self.assertTrue(all(cookie["domain"] == ".reddit.com" for cookie in cookies))
        self.assertTrue(cookies[0]["httpOnly"])
        self.assertFalse(cookies[1]["httpOnly"])

    def test_reddit_cookie_values_remain_empty_in_example_configuration(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        for variable in (
            "REDDIT_SESSION_COOKIE",
            "REDDIT_TOKEN_V2_COOKIE",
            "REDDIT_CSRF_TOKEN_COOKIE",
            "REDDIT_LOID_COOKIE",
            "REDDIT_SESSION_TRACKER_COOKIE",
        ):
            self.assertIn(f"{variable}=\n", env_example)
            self.assertIn(f"{variable}: ${{{variable}:-}}", compose)
            self.assertIn(f"- {variable}=${{{variable}:-}}", compose)


if __name__ == "__main__":
    unittest.main()
