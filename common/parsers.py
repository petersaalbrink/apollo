from datetime import datetime
from pycountry import countries
from urllib.parse import quote_plus
from .handlers import get
from .connectors import EmailClient


def parse(address: str, country: str = "NL"):
    params = {
        "address": quote_plus(address).replace("+", " "),
        "country": country if country == "UK" else countries.lookup(country).name
    }
    response = get(f"http://37.97.136.149:5000/parsers/", params=params, text_only=True)
    if "status" in response:
        EmailClient().send_email(to_address=["esezgin@matrixiangroup.com",
                                             "psaalbrink@matrixiangroup.com"],
                                 subject="VPS11 Address Parser error",
                                 message=f"params = {params}\n"
                                         f"response = {response}")
    return response


def flatten(nested_dict: dict, sep: str = "_"):
    """Flatten a nested dictionary."""

    def _flatten(input_dict):
        flattened_dict = {}
        for key, maybe_nested in input_dict.items():
            if isinstance(maybe_nested, dict):
                for sub, value in maybe_nested.items():
                    flattened_dict[f"{key}{sep}{sub}"] = value
            else:
                flattened_dict[key] = maybe_nested
        return flattened_dict

    return_dict = _flatten(nested_dict)
    while True:
        count = 0
        for v in return_dict.values():
            if not isinstance(v, dict):
                count += 1
        if count == len(return_dict):
            break
        else:
            return_dict = _flatten(return_dict)

    return return_dict


class Checks:
    """Collection of several methods for preparation of MySQL data for MongoDB insertion."""

    @staticmethod
    def percentage(part, whole):
        return round(100 * float(part) / float(whole), 2)

    @staticmethod
    def int_or_null(var):
        try:
            return int(var) if var is not None else None
        except ValueError:
            return None

    @staticmethod
    def bool_or_null(var):
        return bool(Checks.int_or_null(var)) if var is not None else None

    @staticmethod
    def str_or_null(var):
        return str(var) if var else None

    @staticmethod
    def str_or_empty(var):
        return str(var) if var else ""

    @staticmethod
    def float_or_null(var):
        try:
            return float(var) if var is not None else None
        except ValueError:
            return None

    @staticmethod
    def date_or_null(var, f):
        return datetime.strptime(var, f) if var else None

    @staticmethod
    def check_null(var):
        return var if var else None

    @staticmethod
    def energy_label(var):
        return {
            "A": 7,
            "A+": 7,
            "A++": 7,
            "A+++": 7,
            "A++++": 7,
            "B": 6,
            "C": 5,
            "D": 4,
            "E": 3,
            "F": 2,
            "G": 1,
            None: 0
        }.get(var, 0)
