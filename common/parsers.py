from datetime import datetime
from typing import Any, Optional, Union
from dateutil.parser import parse
from numpy import zeros
from pandas import isna


def flatten(nested_dict: dict, sep: str = "_") -> dict:
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


def levenshtein(seq1: str, seq2: str, measure: str = "percentage") -> Union[float, int]:
    """Calculate the Levenshtein distance and score for two strings.

    By default, returns the percentage score.
    Set :param measure: to "distance" to return the Levenshtein distance.
    """
    measures = {"distance", "percentage"}
    if measure not in measures:
        raise ValueError(f"measure should be one of {measures}")

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
        raise ValueError(f"Couldn't find date divider in {date}")
    year_first = date.find(f"{parse(date).year}") == 0
    if year_first:
        fmt = f"%Y{div}%m{div}%d"
    else:
        fmt = f"%d{div}%m{div}%Y"
    return fmt
