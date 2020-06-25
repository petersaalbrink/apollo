from contextlib import suppress
from functools import lru_cache
from json import loads
from socket import gethostname
from time import localtime, sleep
from typing import Union

from phonenumbers import is_valid_number, parse as phoneparse
from phonenumbers.phonenumberutil import NumberParseException
from requests.exceptions import RetryError
from urllib3.exceptions import MaxRetryError

from ..connectors.mx_elastic import ESClient, ElasticsearchException
from ..exceptions import PhoneApiError
from ..persondata import HOST
from ..requests import get
from ..secrets import get_secret

_vn = None
_SECRET = get_secret("MX_WEBHOOK_DATATEAM")
CALL_TO_VALIDATE = True
RESPECT_HOURS = True
VN_INDEX = "cdqc.validated_numbers"
if gethostname() == "matrixian":
    URL = "http://localhost:5000/call/"
else:
    URL = "http://94.168.87.210:4000/call/"


@lru_cache
def check_phone(
        number: Union[int, str],
        country: str = "NL",
        valid: bool = False,
) -> Union[dict, bool]:
    """Use Matrixian's Phone Checker API to validate a phone number.

    Optionally, set valid to True for boolean output.

    Doesn't call between 22PM and 8AM; the
    function will be suspended instead.
    """

    global _vn

    if not (isinstance(country, str) and len(country) == 2):
        raise PhoneApiError("Provide two-letter country code.")

    try:
        parsed = phoneparse(f"{number}", country)
        is_valid = is_valid_number(parsed)
        phone = f"+{parsed.country_code}{parsed.national_number}"
    except NumberParseException:
        is_valid = False
        phone = number

    if not is_valid or country != "NL" or f"{number}".startswith("6"):
        return is_valid if valid else {"phone": phone, "valid": is_valid}
    if country == "NL" and f"{parsed.national_number}".startswith(("8", "9")):  # noqa
        return False if valid else {"phone": phone, "valid": False}

    with suppress(ElasticsearchException):
        query = {"query": {"bool": {"must": {"term": {"phoneNumber": parsed.national_number}}}}}
        try:
            result = _vn.find(query=query, first_only=True)
        except AttributeError:
            _vn = ESClient(VN_INDEX, host=HOST)
            _vn.index_exists = True
            result = _vn.find(query=query, first_only=True)
        if result:
            return result["valid"] if valid else {"phone": phone, "valid": result["valid"]}
    if CALL_TO_VALIDATE:
        if RESPECT_HOURS:
            t = localtime().tm_hour
            while t >= 22 or t < 8:
                sleep(60)
                t = localtime().tm_hour
        while True:
            with suppress(RetryError, MaxRetryError):
                response = get(f"{URL}{phone}", auth=_SECRET)
                if response.ok:
                    is_valid = loads(response.text)
                    break
    return is_valid if valid else {"phone": phone, "valid": is_valid}
