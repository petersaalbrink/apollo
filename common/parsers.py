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


def levenshtein(seq1: str, seq2: str, measure: str = "percentage") -> float:
    """Calculate the Levenshtein distance and score for two strings.

    By default, returns the percentage score.
    Set :param measure: to "distance" to return the Levenshtein distance.
    """
    measures = {"distance", "percentage"}
    if measure not in measures:
        raise ValueError(f"measure should be one of {measures}")
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = zeros((size_x, size_y))
    for _x in range(size_x):
        matrix[_x, 0] = _x
    for _y in range(size_y):
        matrix[0, _y] = _y
    for _x in range(1, size_x):
        for _y in range(1, size_y):
            if seq1[_x - 1] == seq2[_y - 1]:
                matrix[_x, _y] = min(
                    matrix[_x - 1, _y] + 1,
                    matrix[_x - 1, _y - 1],
                    matrix[_x, _y - 1] + 1
                )
            else:
                matrix[_x, _y] = min(
                    matrix[_x - 1, _y] + 1,
                    matrix[_x - 1, _y - 1] + 1,
                    matrix[_x, _y - 1] + 1
                )
    if measure == "percentage":
        return 1 - (matrix[size_x - 1, size_y - 1]) / max(len(seq1), len(seq2))
    else:
        return int(matrix[size_x - 1, size_y - 1])


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
