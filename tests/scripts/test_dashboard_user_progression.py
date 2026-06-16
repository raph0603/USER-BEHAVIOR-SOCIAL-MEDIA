import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class DashboardUserProgressionTests(unittest.TestCase):
    def test_dashboard_exposes_user_progression_metrics(self):
        source = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")

        expected = [
            "Suivi par identifiant",
            "Taux de réponses",
            "Engagement moyen",
            "Événements cumulés",
            "Likes cumulés",
            "Vues cumulées",
            "Réponses cumulées",
            "Classer les identifiants par",
        ]
        for value in expected:
            with self.subTest(value=value):
                self.assertIn(value, source)


if __name__ == "__main__":
    unittest.main()
