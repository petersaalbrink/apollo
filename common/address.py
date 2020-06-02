from typing import Union
from pycountry import countries
from requests.exceptions import HTTPError
from .requests import get
from .connectors import EmailClient

LIVE = "136.144.203.100"
TEST = "136.144.209.80"


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
    try:
        response = get(
            f"http://{LIVE}:5000/parser",
            params=params,
            text_only=True,
        )
    except HTTPError as e:
        response = {"status": f"{e}"}
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

    try:
        response = get(
            f"http://{LIVE}:5000/validation",
            params=params,
            text_only=True
        )["objects"][0]
        return response
    except HTTPError:
        return {}
