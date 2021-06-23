"""This module contains api connectors for Matrixian Group.

Use `address` for the Address Checker API
Use `email` for the Email Checker API
Use `phone` for the Phone Checker API
"""

from __future__ import annotations

__all__ = (
    "check_email",
    "check_phone",
    "parse",
    "validate",
)

from importlib import import_module
from types import ModuleType

_module_mapping = {
    "address": [
        "parse",
        "validate",
    ],
    "email": [
        "check_email",
    ],
    "phone": [
        "check_phone",
    ],
}


def __getattr__(name: str) -> ModuleType:
    if name in _module_mapping:
        return import_module(f".{name}", __name__)
    for module, symbols in _module_mapping.items():
        if name in symbols:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
