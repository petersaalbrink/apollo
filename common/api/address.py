__all__ = (
    "parse",
    "validate",
)

from functools import lru_cache
import logging
from typing import Union

from pycountry import countries
import urllib3

from ..requests import get

LIVE = "136.144.203.100"
TEST = "136.144.209.80"
PARSER = f"https://{TEST}:5000/parser"
VALIDATION = f"https://{TEST}:5000/validation"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@lru_cache()
def parse(address: str, country: str = None):
    """
    Parses an address in a string format and returns address elements in JSON.

    Example:
        from common.api.address import parse
        PARSED_ADDRESS = parse("Transformatorweg 102")
        print(dumps(PARSED_ADDRESS, indent=2, ensure_ascii=False))

    :param address: Address input you want parsed
    :param country: ISO2 country code of the input address
    :return: address elements in JSON
    """

    for s in ("p.a. ", "P.a. ", "p/a ", "P/a "):
        address = address.replace(s, "")
    if not country:
        country = "NLD"
    elif not (country.isupper() and len(country) == 3):
        country = {
            "NL": "NLD",
            "UK": "GBR",
            "United Kingdom": "GBR"
        }.get(country, countries.lookup(country).alpha_3)
    params = {
        "address": address,
        "country": country,
    }
    response = get(
        PARSER,
        params=params,
        verify=False,
        text_only=True,
    )
    if isinstance(response, list):
        logging.warning("%s", response)
        response = {}
    return response


def validate(params: Union[dict, str]) -> dict:
    """
    Uses the address checker API to validate an address.

    Example:
        from common.api.address import validate
        from json import dumps

        PARAMS = {
            "input_street": "Transformatorweg",
            "input_housenumber": "104",
            "input_subBuilding": "",
            "input_postcode": "1014AK",
            "input_city": "Amsterdam",
            "country": "NL"
        }

        print(dumps(validate(PARAMS), indent=2))

    :param params: Dictionary containing the address elements you want to validate
    :return:  JSON object containing validated address
    """
    if isinstance(params, str):
        params = {"input_street": params}

    if "country" not in params:
        params["country"] = "NL"
    if "script" not in params:
        params["script"] = "POSTNL"

    keys = (
        "input_street",
        "input_building",
        "input_housenumber",
        "input_subBuilding",
        "input_postcode",
        "input_city",
    )
    for key in keys:
        if key.lstrip("input_") in params:
            params[key] = params.pop(key.lstrip("input_"))
        if key not in params:
            params[key] = ""

    response = get(
        VALIDATION,
        verify=False,
        params=params,
        text_only=True,
    )
    try:
        return response["objects"][0]
    except KeyError:
        logging.warning("%s", response)
        return {}
