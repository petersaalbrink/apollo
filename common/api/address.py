from __future__ import annotations

__all__ = (
    "parse",
    "validate",
)

from typing import Any, NoReturn

LIVE = "136.144.203.100"
TEST = "136.144.209.80"
PARSER = f"https://{TEST}:5000/parser"
VALIDATION = f"https://{TEST}:5000/validation"


def parse(*_: Any, **__: Any) -> NoReturn:
    raise DeprecationWarning("Use `iac` package instead.")


def validate(*_: Any, **__: Any) -> NoReturn:
    raise DeprecationWarning("Use `iac` package instead.")
