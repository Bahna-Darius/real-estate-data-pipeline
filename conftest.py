"""
conftest.py — pytest root configuration.
Ensures the project root is on sys.path so `from src.x import y` works
with both `pytest` and `python -m pytest`.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "src" / "pipeline"))
