"""Test package for pydfs."""

from __future__ import annotations

import sys
from pathlib import Path


# Ensure the src/ directory is importable when running ``pytest`` without
# installing the project in editable mode. This mirrors the layout used by the
# application package while keeping the test invocation lightweight for CI.
SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if SRC_ROOT.exists():
    src_str = str(SRC_ROOT)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
