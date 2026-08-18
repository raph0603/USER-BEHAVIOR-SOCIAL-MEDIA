"""Compatibility entrypoint for the official Stage-1 grouped-CV workflow."""

from __future__ import annotations

try:
    from .grouped_cv_stage1 import *  # noqa: F403
    from .grouped_cv_stage1 import main
except ImportError:  # direct script execution
    from grouped_cv_stage1 import *  # type: ignore  # noqa: F403
    from grouped_cv_stage1 import main  # type: ignore


if __name__ == "__main__":
    main()
