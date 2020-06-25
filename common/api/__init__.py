"""This module contains api connectors for Matrixian Group.

Use `address` for the Address Checker API
Use `email` for the Email Checker API
Use `phone` for the Phone Checker API
"""
from .address import parse, validate
from .email import check_email
from .phone import check_phone
