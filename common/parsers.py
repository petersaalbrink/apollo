from datetime import datetime
from typing import Any, List, MutableMapping, Optional, Union
from dateutil.parser import parse
from numpy import zeros
from pandas import isna
from text_unidecode import unidecode
from .exceptions import ParseError


def _flatten(input_dict: MutableMapping[str, Any], sep: str):
    flattened_dict = {}
    for key, maybe_nested in input_dict.items():
        if isinstance(maybe_nested, dict):
            for sub, value in maybe_nested.items():
                flattened_dict[f"{key}{sep}{sub}"] = value
        else:
            flattened_dict[key] = maybe_nested
    return flattened_dict


def flatten(nested_dict: MutableMapping[str, Any], sep: str = "_") -> dict:
    """Flatten a nested dictionary."""

    return_dict = _flatten(nested_dict, sep)
    while True:
        count = 0
        for v in return_dict.values():
            if not isinstance(v, MutableMapping):
                count += 1
        if count == len(return_dict):
            break
        else:
            return_dict = _flatten(return_dict, sep)

    return return_dict


class Checks:
    """Collection of several methods for preparation of MySQL data for MongoDB insertion."""

    @staticmethod
    def percentage(part, whole) -> float:
        return round(100 * float(part) / float(whole), 2)

    @staticmethod
    def int_or_null(var) -> Optional[int]:
        try:
            return int(var) if var and not isna(var) else None
        except ValueError:
            return None

    @staticmethod
    def bool_or_null(var) -> Optional[bool]:
        return bool(Checks.int_or_null(var)) if var and not isna(var) else None

    @staticmethod
    def str_or_null(var) -> Optional[str]:
        return str(var) if var and not isna(var) else None

    @staticmethod
    def str_or_empty(var) -> str:
        return str(var) if var and not isna(var) else ""

    @staticmethod
    def float_or_null(var) -> Optional[float]:
        try:
            return float(var) if var and not isna(var) else None
        except ValueError:
            return None

    @staticmethod
    def date_or_null(var, f) -> Optional[datetime]:
        return datetime.strptime(var, f) if var and not isna(var) else None

    @staticmethod
    def check_null(var) -> Optional[Any]:
        return var if var and not isna(var) else None

    @staticmethod
    def energy_label(var) -> int:
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

    @staticmethod
    def remove_invalid_chars(text: str, replacechar: str) -> str:
        for c in r"\`*_{}[]()>#+.&!$,":
            text = text.replace(c, replacechar)
        return text

    def check_matching_percentage(self, str1: str, str2: str) -> int:
        str1 = self.remove_invalid_chars(
            unidecode(str1), "").replace(" ", "").lower().strip()
        str2 = self.remove_invalid_chars(
            unidecode(str2), "").replace(" ", "").lower().strip()
        lev = levenshtein(str1, str2, measure="percentage")
        return int(lev * 100)


def levenshtein(seq1: str, seq2: str, measure: str = "percentage") -> Union[float, int]:
    """Calculate the Levenshtein distance and score for two strings.

    By default, returns the percentage score.
    Set :param measure: to "distance" to return the Levenshtein distance.
    """
    measures = {"distance", "percentage"}
    if measure not in measures:
        raise ParseError(f"measure should be one of {measures}")

    size_1, size_2 = len(seq1), len(seq2)
    size_1p, size_2p = size_1 + 1, size_2 + 1

    distances = zeros((size_1p, size_2p))

    for t1 in range(size_1p):
        distances[t1][0] = t1

    for t2 in range(size_2p):
        distances[0][t2] = t2

    for t1 in range(1, size_1p):
        for t2 in range(1, size_2p):
            if seq1[t1 - 1] == seq2[t2 - 1]:
                distances[t1][t2] = distances[t1 - 1][t2 - 1]
            else:
                a = distances[t1][t2 - 1]
                b = distances[t1 - 1][t2]
                c = distances[t1 - 1][t2 - 1]

                if c >= a <= b:
                    distances[t1][t2] = a + 1
                elif c >= b <= a:
                    distances[t1][t2] = b + 1
                else:
                    distances[t1][t2] = c + 1

    if measure == "percentage":
        return 1 - (distances[size_1, size_2]) / max(size_1, size_2)
    else:
        return int(distances[size_1][size_2])


def dateformat(date: str) -> str:
    """Parse a date from a datestring and return the format.
    Example::
        dateformat("28/08/2014") == "%d/%m/%Y"
    """
    for div in "/-":
        if div in date:
            break
    else:
        raise ParseError(f"Couldn't find date divider in {date}")
    year_first = date.find(f"{parse(date).year}") == 0
    if year_first:
        fmt = f"%Y{div}%m{div}%d"
    else:
        fmt = f"%d{div}%m{div}%Y"
    return fmt


def expand(data: List[dict]) -> List[dict]:
    """Standardize irregular data, e.g. for writing to CSV or SQL.

    This function accepts a list of dictionaries, and transforms it so
    that all dictionaries have the same keys and in the same order.

    Example::
        from common.parsers import expand
        data = expand(data)
    """
    fieldnames = {field for doc in data for field in doc}
    for doc in data:
        for field in fieldnames:
            if field not in doc:
                doc[field] = None
    data = [dict(sorted(d.items())) for d in data]
    return data


def drop_empty_columns(data: List[dict]) -> List[dict]:
    """Remove keys that have no True-y value for all entries.

    This function accepts a list of dictionaries, and transforms it so
    that keys that have a value that evaluates to False for all dicts
    will be removed from the list.

    Example::
        from common.parsers import drop_empty_columns
        data = drop_empty_columns(data)
    """
    fieldnames = set(data[0].keys())
    for doc in data:
        for field in tuple(fieldnames):
            if doc[field]:
                fieldnames.remove(field)
        if not fieldnames:
            break
    else:
        for doc in data:
            for field in fieldnames:
                del doc[field]
    return data
