"""Shared test fixtures."""
import sys
from pathlib import Path

# Make src/ importable without installing, and make the repo root importable
# so cross-test imports like `from tests.test_scan_sh import _build_fake_tree`
# work.
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
