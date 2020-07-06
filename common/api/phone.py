from contextlib import suppress
from dataclasses import astuple, dataclass
from functools import lru_cache
from socket import gethostname
from time import localtime, sleep
from typing import Optional, Union

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
_SECRET = None
CALL_TO_VALIDATE = True
RESPECT_HOURS = True
_WRONG_CHARS = (".0", "%2B")
_WRONG_NUMS = ("8", "9", "66", "67", "69", "60")
VN_INDEX = "cdqc.validated_numbers"
if gethostname() == "matrixian":
    URL = "http://localhost:5000/call/"
else:
    URL = "http://94.168.87.210:4000/call/"


@dataclass
class ParsedPhoneNumber:
    __slots__ = [
        "country_code",
        "national_number",
        "parsed_number",
        "is_valid_number",
    ]
    country_code: int
    national_number: int
    parsed_number: str
    is_valid_number: bool

    def __bool__(self):
        return any(astuple(self))

    def __hash__(self):
        return hash(astuple(self))


def _return(
        parsed: ParsedPhoneNumber,
        valid: bool,
) -> Union[dict, bool]:
    return (
        parsed.is_valid_number
        if valid else
        {"phone": parsed.parsed_number,
         "valid": parsed.is_valid_number}
    )


@lru_cache()
def parse_phone(
        number: Union[int, str],
        country: str = None,
) -> Optional[ParsedPhoneNumber]:

    if not ((isinstance(country, str) and len(country) == 2)
            or f"{number}".startswith("+")):
        raise PhoneApiError("Provide two-letter country code.")

    # Clean up
    number = f"{number}"
    for s in _WRONG_CHARS:
        number = number.replace(s, "")

    try:
        # Parse the number
        parsed = phoneparse(number, country)

        # Validate the number
        if parsed.country_code == 31 and f"{parsed.national_number}".startswith(_WRONG_NUMS):
            valid = False
        else:
            valid = is_valid_number(parsed)

        return ParsedPhoneNumber(
            parsed.country_code,
            parsed.national_number,
            f"+{parsed.country_code}{parsed.national_number}",
            valid,
        )

    except NumberParseException:
        raise PhoneApiError(f"Incorrect number for country '{country}': {number}")


@lru_cache()
def lookup_phone(
        parsed: ParsedPhoneNumber,
        valid: bool,
) -> Optional[Union[dict, bool]]:

    global _vn

    with suppress(ElasticsearchException):

        if not _vn:
            _vn = ESClient(VN_INDEX, host=HOST)
            _vn.index_exists = True

        result = _vn.find(
            query={"query": {"bool": {"must": {"term": {"phoneNumber": parsed.national_number}}}}},
            first_only=True,
        )
        if result:
            parsed.is_valid_number = result["valid"]
            return _return(parsed, valid)


@lru_cache()
def call_phone(
        parsed: ParsedPhoneNumber,
        valid: bool,
) -> Union[dict, bool]:

    global _SECRET

    if CALL_TO_VALIDATE:

        if not _SECRET:
            _SECRET = get_secret("MX_WEBHOOK_DATATEAM")

        if RESPECT_HOURS:
            t = localtime().tm_hour
            while t >= 22 or t < 8:
                sleep(60)
                t = localtime().tm_hour

        while True:
            with suppress(RetryError, MaxRetryError):
                response = get(f"{URL}{parsed.parsed_number}", auth=_SECRET)
                if response.ok:
                    parsed.is_valid_number = response.json()["valid"]
                    break

    return _return(parsed, valid)


@lru_cache()
def check_phone(
        number: Union[int, str],
        country: str = None,
        valid: bool = False,
        call: bool = False,
) -> Union[dict, bool]:
    """Use Matrixian's Phone Checker API to validate a phone number.

    :param number: the phone number to validate.

    Optional arguments:
        :param country: default "NL"; set the country if the number does not
            contain a country code.
        :param valid: default False; set to True for boolean output.
        :param call: default False; set to True to call Dutch landlines.
            Doesn't call between 22PM and 8AM; the function will be suspended
            instead.
    """

    if country is None and (
            not isinstance(number, str)
            or not f"{number}".startswith("+")):
        country = "NL"
    parsed = parse_phone(number, country)

    if (not parsed.is_valid_number
            or parsed.country_code != 31
            or f"{parsed.national_number}".startswith("6")):
        return _return(parsed, valid)

    result = lookup_phone(parsed, valid)
    if result:
        return result

    if call:
        return call_phone(parsed, valid)
    return _return(parsed, valid)
