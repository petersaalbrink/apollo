from dataclasses import astuple, dataclass
from datetime import datetime
from functools import lru_cache
from socket import gethostname
from time import localtime, sleep
from typing import Optional, Union

from phonenumbers import is_valid_number, parse, PhoneNumber
from phonenumbers.carrier import name_for_number, number_type  # noqa
from phonenumbers.geocoder import country_name_for_number
from phonenumbers.phonenumberutil import NumberParseException
from pycountry import countries

from ..connectors.mx_elastic import ESClient
from ..exceptions import PhoneApiError
from ..persondata import HOST
from ..requests import post
from ..secrets import get_secret

from ._acm import ACM

_SECRET = None
_WRONG_NUMS = ("9", "66", "67", "69", "60")
_vn = None
_acm = None
CALL_TO_VALIDATE = True
RESPECT_HOURS = True
VN_INDEX = "cdqc.validated_numbers"
TYPES = {
    0: "landline",
    1: "mobile",
}
if gethostname() == "matrixian":
    URL = "http://localhost:5000/call/"
else:
    URL = "http://94.168.87.210:4000/call/"


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
    valid_number: bool = True

    def __hash__(self):
        return hash(astuple(self))

    @classmethod
    def from_parsed(cls, parsed: PhoneNumber):
        carrier = name_for_number(parsed, "en") or None
        try:
            country = countries.lookup(country_name_for_number(parsed, "en"))
        except LookupError as e:
            if parsed.country_code == 39:
                country = countries.lookup("Italy")
            else:
                raise PhoneApiError(f"No country for: {parsed}") from e
        valid = is_valid_number(parsed)
        return cls(
            country_code=parsed.country_code,
            country_iso2=country.alpha_2,
            country_iso3=country.alpha_3,
            country_name=country.name,
            current_carrier=carrier,
            national_number=parsed.national_number,
            number_type=TYPES.get(number_type(parsed)),
            original_carrier=carrier,
            parsed_number=f"+{parsed.country_code}{parsed.national_number}",
            valid_format=valid,
            valid_number=valid,
        )


@lru_cache()
def parse_phone(
        number: Union[int, str],
        country: str = None,
) -> Optional[PhoneApiResponse]:

    if not (isinstance(country, str) or f"{number}".startswith("+")):
        raise PhoneApiError("Provide country code or international number.")

    # Clean up
    number = f"{number}".replace(".0", "").replace(".", "").replace("%2B", "+")

    # Parse the number
    try:
        parsed = parse(number, country)
    except NumberParseException:
        try:
            parsed = parse(number, countries.lookup(country).alpha_2)
        except (NumberParseException, LookupError):
            raise PhoneApiError(f"Incorrect number for country '{country}': {number}")

    # Create object
    phone = PhoneApiResponse.from_parsed(parsed)

    # Set number to invalid for some cases
    if phone.country_code == 31 and f"{phone.national_number}".startswith(_WRONG_NUMS):
        phone.valid_number = False

    return phone


@lru_cache()
def lookup_carriers_acm(
        phone: PhoneApiResponse,
) -> PhoneApiResponse:
    global _acm
    try:
        phone = _acm.get_acm_data(phone)
    except AttributeError:
        _acm = ACM()
        phone = _acm.get_acm_data(phone)
    return phone


@lru_cache()
def lookup_call_result(
        phone: PhoneApiResponse,
) -> Optional[PhoneApiResponse]:

    global _vn

    if not _vn:
        _vn = ESClient(VN_INDEX, host=HOST)
        _vn.index_exists = True

    query = {"query": {"bool": {"filter": {"term": {"phoneNumber": phone.national_number}}}}}
    result = _vn.find(query, size=1, source_only=True)
    if result:
        phone.valid_number = result["valid"]
        return phone


@lru_cache()
def call_phone(
        phone: PhoneApiResponse,
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

        response = post(
            url=URL,
            auth=_SECRET,
            data=f"{phone}".encode(),
        )
        phone = eval(response.content)
        if not isinstance(phone, PhoneApiResponse):
            raise PhoneApiError(type(phone))

    return phone


@lru_cache()
def check_phone(
        number: Union[int, str],
        country: str = None,
        call: bool = False,
        acm: bool = False,
) -> PhoneApiResponse:
    """Use Matrixian's Phone Checker API to validate a phone number.

    :param number: the phone number to validate.

    Optional arguments:
        :param country: default "NL"; set the country if the number does not
            contain a country code.
        :param call: default False; set to True to call Dutch landlines.
            Doesn't call between 22PM and 8AM; the function will be suspended
            instead.
        :param acm: default False; set to True to scrape Dutch carrier (ACM) data.
    """

    if country is None and (
            not isinstance(number, str)
            or not f"{number}".startswith("+")):
        country = "NL"
    phone = parse_phone(number, country)

    if (not phone.valid_number
            or phone.country_code != 31):
        return phone

    if acm:
        phone = lookup_carriers_acm(phone)
    result = lookup_call_result(phone)
    if result:
        return result

    if call and not phone.number_type == "mobile":
        return call_phone(phone)

    return phone


def cache_clear():
    """Convenience function to clear the cache for this module's functions."""
    call_phone.cache_clear()
    check_phone.cache_clear()
    lookup_call_result.cache_clear()
    lookup_carriers_acm.cache_clear()
    parse_phone.cache_clear()
