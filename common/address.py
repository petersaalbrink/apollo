from logging import debug
from urllib.parse import quote_plus
from pycountry import countries
from .requests import get
from .connectors import EmailClient


def parse(address: str, country: str = "NL"):
    params = {
        "address": quote_plus(address).replace("+", " "),
        "country": {
            "UK": "UK",
            "United Kingdom": "UK"
        }.get(country, countries.lookup(country).name)
    }
    response = get(f"http://136.144.209.80:5000/parser", params=params, text_only=True)
    if "status" in response:
        EmailClient().send_email(to_address=["esezgin@matrixiangroup.com",
                                             "psaalbrink@matrixiangroup.com"],
                                 subject="VPS11 Address Parser error",
                                 message=f"params = {params}\n"
                                         f"response = {response}")
    return response


def validate(params: dict):
    while True:
        try:
            response = get(
                "http://136.144.203.100:5000/validation",
                params=params, text_only=True)["objects"][0]
            break
        except Exception as e:
            debug("Exception: %s: %s", params, e)
    return response
