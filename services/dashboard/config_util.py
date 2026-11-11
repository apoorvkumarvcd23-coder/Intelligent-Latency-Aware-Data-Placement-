"""Compatibility entry point; validation lives in one shared module."""
from pathlib import Path
import sys

root = Path(__file__).resolve().parent
if not (root / 'common').is_dir():
    root = root.parents[1]
if (root / 'common').is_dir():
    sys.path.insert(0, str(root))
from common.contracts import load_config  # noqa: E402,F401
