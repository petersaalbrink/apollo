from logging import debug
from typing import Union
from urllib3.exceptions import HTTPError
from pycountry import countries
from .requests import get
from .connectors import EmailClient


def parse(address: str, country: str = "NL"):
    for s in ("p.a. ", "P.a. ", "p/a ", "P/a "):
        address = address.replace(s, "")
    params = {
        "address": address,
        "country": {
            "NL": "Netherlands",
            "UK": "UK",
            "United Kingdom": "UK"
        }.get(country, countries.lookup(country).name)
    }
    while True:
        try:
            response = get(f"http://136.144.209.80:5000/parser",
                           params=params, text_only=True)
            break
        except (IOError, OSError, HTTPError) as e:
            debug("Exception: %s: %s", params, e)
    if "status" in response:
        EmailClient().send_email(to_address=["esezgin@matrixiangroup.com",
                                             "psaalbrink@matrixiangroup.com"],
                                 subject="Address Parser error",
                                 message=f"params = {params}\n"
                                         f"response = {response}")
    return response


def validate(params: Union[dict, str]) -> dict:

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

    while True:
        try:
            response = get(
                "http://136.144.203.100:5000/validation",
                params=params, text_only=True)["objects"][0]
            break
        except (IOError, OSError, HTTPError) as e:
            debug("Exception: %s: %s", params, e)
    return response
