"""Static (basic) content-feature extractors — pure functions of the post text.

These features are known at pre-launch and never change for a given text, so the same
function is used at training time (notebooks) and serving time (``ml/serve/predict.py``).
"""
from .cognitive_friction import cognitive_friction

__all__ = ["cognitive_friction"]
