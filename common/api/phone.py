from contextlib import suppress
from dataclasses import asdict, astuple, dataclass
from datetime import datetime
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
    country_code: int = None
    national_number: int = None
    parsed_number: str = None
    valid_number: bool = None

    def __hash__(self):
        return hash(astuple(self))


@dataclass
class PhoneApiResponse:
    country_code: int = None
    country_iso2: str = None
    country_iso3: str = None
    country_name: str = None
    current_carrier: str = None
    date_allocation: datetime = None
    date_cooldown: datetime = None
    date_mutation: datetime = None
    date_portation: datetime = None
    national_number: int = None
    number_status: str = None
    number_type: str = None
    original_carrier: str = None
    parsed_number: str = None
    status: str = "OK"
    valid_format: bool = None
    valid_number: bool = None

    @classmethod
    def from_parsed(cls, parsed: ParsedPhoneNumber):
        return cls(**asdict(parsed))


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
) -> Optional[PhoneApiResponse]:

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
            parsed.valid_number = result["valid"]
            return PhoneApiResponse.from_parsed(parsed)


@lru_cache()
def call_phone(
        parsed: ParsedPhoneNumber,
) -> PhoneApiResponse:

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
                    parsed.valid_number = response.json()["valid"]
                    break

    return PhoneApiResponse.from_parsed(parsed)


@lru_cache()
def check_phone(
        number: Union[int, str],
        country: str = None,
        call: bool = False,
) -> PhoneApiResponse:
    """Use Matrixian's Phone Checker API to validate a phone number.

    :param number: the phone number to validate.

    Optional arguments:
        :param country: default "NL"; set the country if the number does not
            contain a country code.
        :param call: default False; set to True to call Dutch landlines.
            Doesn't call between 22PM and 8AM; the function will be suspended
            instead.
    """

    if country is None and (
            not isinstance(number, str)
            or not f"{number}".startswith("+")):
        country = "NL"
    parsed = parse_phone(number, country)

    if (not parsed.valid_number
            or parsed.country_code != 31
            or f"{parsed.national_number}".startswith("6")):
        return PhoneApiResponse.from_parsed(parsed)

    result = lookup_phone(parsed)
    if result:
        return result

    if call:
        return call_phone(parsed)
    return PhoneApiResponse.from_parsed(parsed)


def cache_clear():
    """Convenience function to clear the cache for this module's functions."""
    call_phone.cache_clear()
    check_phone.cache_clear()
    lookup_phone.cache_clear()
    parse_phone.cache_clear()
