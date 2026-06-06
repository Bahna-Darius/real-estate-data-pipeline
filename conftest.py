"""
conftest.py — pytest root configuration.
Ensures the project root is on sys.path so `from src.x import y` works
with both `pytest` and `python -m pytest`.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
