from __future__ import annotations

from pathlib import Path

with open(Path(__file__).parent / "_version.txt") as f:
    __version__ = f.read().strip()
