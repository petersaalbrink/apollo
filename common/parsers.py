from address_checker.Address_Parsers.address_parser import parser_final as global_parser
from address_checker.Address_Parsers.address_parser_BE import parser_final as belgium_parser
from address_checker.Address_Parsers.address_parser_DE import parser_final as german_parser
from address_checker.Address_Parsers.address_parser_FR import parser as french_parser
from address_checker.Address_Parsers.address_parser_IT import parser as italian_parser
from address_checker.Address_Parsers.address_parser_NL import parser_final as dutch_parser
from address_checker.Address_Parsers.address_parser_UK import parser as english_parser


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
