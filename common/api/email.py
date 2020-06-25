from functools import lru_cache
from typing import Union

from ..requests import get

from .address import LIVE

JSON = {
    "true": True,
    "false": False,
}
URL = f"http://{LIVE}:4000/email?email="


@lru_cache
def check_email(
        email: str,
        safe_to_send: bool = False,
) -> Union[dict, bool]:
    """Use Matrixian's Email Checker API to validate an email address.

    Optionally, set safe_to_send to True for boolean output.
    """
    response = get(
        f"{URL}{email}",
        text_only=True,
        timeout=10,
    )
    for k, v in response.items():
        if v in JSON:
            response[k] = JSON[v]
    if safe_to_send:
        return response["safe_to_send"]
    return response
