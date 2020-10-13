"""This module contains api connectors for Matrixian Group.

Use `address` for the Address Checker API
Use `email` for the Email Checker API
Use `phone` for the Phone Checker API
"""
from importlib import import_module

__mapping__ = {
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
__modules__ = list(__mapping__)
__all__ = list(__mapping__.values()) + __modules__


def __getattr__(name):
    if name in __modules__:
        return import_module(f".{name}", __name__)
    for module, symbols in __mapping__.items():
        if name in symbols:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
