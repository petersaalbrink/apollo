from datetime import datetime
from address_checker.Global.address_parser import parser_final as global_parser
from address_checker.BE.Parser.address_parser_BE import parser_final as belgium_parser
from address_checker.DE.Parser.address_parser_DE import parser_final as german_parser
from address_checker.FR.Parser.address_parser_FR import parser as french_parser
from address_checker.IT.Parser.address_parser_IT import parser as italian_parser
from address_checker.NL.Parser.address_parser_NL import parser_final as dutch_parser
from address_checker.UK.Parser.address_parser_UK import parser as english_parser


def parse(address: str, country: str = "NL"):
    return {
        "global": global_parser(address),
        "BE": belgium_parser(address),
        "DE": german_parser(address),
        "FR": french_parser(address),
        "IT": italian_parser(address),
        "NL": dutch_parser(address),
        "UK": english_parser(address),
    }.get(country, "global")


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
        return float(var) if var is not None else None

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
