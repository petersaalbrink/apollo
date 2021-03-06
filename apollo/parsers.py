"""Module for data/object operations and calculations.

This module contains the following objects:

.. py:class:: apollo.parsers.Checks
   Collection of several methods for data transformation.
   .. py:method:: percentage()
      Transform a part of a whole into a percentage (0-100).
   .. py:method:: int_or_null()
      Transform to an integer, if possible.
   .. py:method:: bool_or_null()
      Transform to a boolean, if possible.
   .. py:method:: str_or_null()
      Transform to a string, if possible.
   .. py:method:: str_or_empty()
      Always transform to a string.
   .. py:method:: float_or_null()
      Transform to a floating point, if possible.
   .. py:method:: date_or_null()
      Transform to a datetime, if possible.
   .. py:method:: check_null()
      Check if data resolves to True, otherwise return None.
   .. py:method:: energy_label()
      Convert an energy label to a corresponding number.
   .. py:method:: remove_invalid_chars()
      Replace special characters in a string.
   .. py:method:: check_matching_percentage()
      Return matching percentage (0-100) of two strings.

.. py:function:: apollo.parsers.flatten(
        nested_dict: MutableMapping[str, Any],
        sep: str = "_",
    ) -> dict
   Flatten a nested dictionary.

.. py:function:: apollo.parsers.levenshtein(
        seq1: str,
        seq2: str,
        measure: str = "percentage",
    ) -> Union[float, int]
   Calculate the Levenshtein distance and score for two strings.

   By default, returns the percentage score.
   Set :param measure: to "distance" to return the Levenshtein distance.

.. py:function:: apollo.parsers.date(date: str) -> str
   Parse a date from a datestring and return the format.
   Example::
        dateformat("28/08/2014") == "%d/%m/%Y"

.. py:function:: apollo.parsers.find_all_urls(text: str) -> list
   Finds all urls in a string and returns a list
   Example::
        urls = find_all_urls(text)

.. py:function:: apollo.parsers.reverse_geocode(x: float, y: float) -> dict
   Returns address from x and y coordinates using ArcGIS; reverse geocoding.
   Example::
        address = reverse_geocode(4.894410, 52.310158)

.. py:function:: apollo.parsers.partition(
        pred: Callable[[T], bool],
        it: Iterable[T],
    ) -> tuple[list[T], list[T]]
   Split an iterable `it` into two lists using some predicate `pred`.
   Example::
        evens, odds = partition(lambda num: (num % 2) == 0, range(100))

.. py:function:: apollo.parsers.count_bytes(num: int) -> int
   Calculate the number of bytes needed to store `num` as a signed integer.
   Example::
        print(count_bytes(127), count_bytes(128))
"""

from __future__ import annotations

__all__ = (
    "Checks",
    "DISTANCE",
    "PERCENTAGE",
    "dateformat",
    "drop_empty_columns",
    "count_bytes",
    "expand",
    "flatten",
    "levenshtein",
    "partition",
    "reverse_geocode",
)

import datetime
from functools import lru_cache
from re import compile, findall
from typing import Any, Callable, Iterable, TypeVar
from warnings import warn

from dateutil.parser import parse
from numpy import zeros
from pandas import isna, notna
from requests import Response
from text_unidecode import unidecode

from .exceptions import ParseError
from .requests import get

T = TypeVar("T")


def _flatten(input_dict: dict[str, Any], sep: str) -> dict[str, Any]:
    flattened_dict = {}
    for key, maybe_nested in input_dict.items():
        if isinstance(maybe_nested, dict):
            for sub, value in maybe_nested.items():
                flattened_dict[f"{key}{sep}{sub}"] = value
        else:
            flattened_dict[key] = maybe_nested
    return flattened_dict


def flatten(nested_dict: dict[str, Any], sep: str = "_") -> dict[str, Any]:
    """Flatten a nested dictionary."""
    __flatten = _flatten
    return_dict = __flatten(nested_dict, sep)
    while True:
        count = 0
        for v in return_dict.values():
            if not isinstance(v, dict):
                count += 1
        if count == len(return_dict):
            break
        else:
            return_dict = __flatten(return_dict, sep)

    return return_dict


class Checks:
    """Collection of several methods for data transformation."""

    @staticmethod
    def percentage(part: int | float | str, whole: int | float | str) -> float:
        """Transform a part of a whole into a percentage (0-100)."""
        return round(100 * float(part) / float(whole), 2)

    @staticmethod
    def int_or_null(var: Any) -> int | None:
        """Transform to an integer, if possible."""
        try:
            return int(var)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def bool_or_null(var: Any) -> bool | None:
        """Transform to a boolean, if possible."""
        return bool(Checks.float_or_null(var))

    @staticmethod
    def str_or_null(var: Any) -> str | None:
        """Transform to a string, if possible."""
        return str(var) if notna(var) else None

    @staticmethod
    def str_or_empty(var: Any) -> str:
        """Always transform to a string."""
        return str(var) if notna(var) else ""

    @staticmethod
    def float_or_null(var: Any) -> float | None:
        """Transform to a floating point, if possible."""
        try:
            return float(var) if notna(var) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def date_or_null(var: Any, f: str) -> datetime.datetime | None:
        """Transform to a datetime, if possible."""
        if isna(var) or not var:
            return None
        elif isinstance(var, str):
            return datetime.datetime.strptime(var, f)
        elif isinstance(var, datetime.datetime):
            return var
        elif isinstance(var, datetime.date):
            return datetime.datetime(var.year, var.month, var.day)
        else:
            raise NotImplementedError

    @staticmethod
    def check_null(var: Any) -> Any | None:
        """Check if data resolves to True, otherwise return None."""
        return var if notna(var) else None

    @staticmethod
    def energy_label(var: str) -> int:
        """Convert an energy label to a corresponding number."""
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
            None: 0,
        }.get(var, 0)

    @staticmethod
    def remove_invalid_chars(text: str, replacechar: str = "") -> str:
        """Replace special characters in a string."""
        for c in r"\`*_{}[]()>#+.&!$,":
            text = text.replace(c, replacechar)
        return text

    @staticmethod
    def check_matching_percentage(str1: str, str2: str) -> int:
        """Return matching percentage (0-100) of two strings."""
        str1 = (
            Checks.remove_invalid_chars(unidecode(str1), "")
            .replace(" ", "")
            .lower()
            .strip()
        )
        str2 = (
            Checks.remove_invalid_chars(unidecode(str2), "")
            .replace(" ", "")
            .lower()
            .strip()
        )
        lev = levenshtein(str1, str2, measure="percentage")
        return int(lev * 100)


DISTANCE = "distance"
PERCENTAGE = "percentage"


@lru_cache
def levenshtein(seq1: str, seq2: str, measure: str = PERCENTAGE) -> float | int:
    """Calculate the Levenshtein distance and score for two strings.

    By default, returns the percentage score.
    Set :param measure: to "distance" to return the Levenshtein distance.
    """
    if measure != DISTANCE and measure != PERCENTAGE:
        raise ParseError(f"wrong measure: {measure}")

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

    if measure == PERCENTAGE:
        return 1 - (distances[size_1, size_2]) / max(size_1, size_2)
    else:
        return int(distances[size_1][size_2])


def dateformat(date: str) -> str:
    """Parse a date from a datestring and return the format.
    Example::
        dateformat("28/08/2014") == "%d/%m/%Y"
    """

    dt = parse(date)

    for div in "-/":
        if div in date:
            break
    else:
        if len(date) == 8:
            div = ""
        else:
            raise ParseError(f"Couldn't find date divider in {date!r}")

    year_first = date.find(f"{dt.year}") == 0
    if year_first:
        month_first = False

    else:
        day_first = date.find(f"{dt.day:02}") == 0
        month_first = date.find(f"{dt.month:02}") == 0

        one = sum((day_first, month_first, year_first))

        if one == 0:
            raise ParseError(f"Couldn't parse date format of {date!r}")
        elif one > 1:
            warn(
                f"Couldn't parse order of day and month in {date!r}"
                f"; assuming day first, then month."
            )
            month_first = False

    if year_first:
        return f"%Y{div}%m{div}%d"
    elif month_first:
        return f"%m{div}%d{div}%Y"
    else:
        return f"%d{div}%m{div}%Y"


def expand(data: list[dict[str, Any]], sort: bool = True) -> list[dict[str, Any]]:
    """Standardize irregular data, e.g. for writing to CSV or SQL.

    This function accepts a list of dictionaries, and transforms it so
    that all dictionaries have the same keys and in the same order.

    Example::
        from apollo.parsers import expand
        data = expand(data)
    """
    fieldnames = {field for doc in data for field in doc}
    for doc in data:
        for field in fieldnames:
            if field not in doc:
                doc[field] = None
    if sort:
        data = [dict(sorted(d.items())) for d in data]  # noqa
    return data


def drop_empty_columns(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove keys that have no value for all entries.

    This function accepts a list of dictionaries, and transforms it so
    that keys that have a value that evaluates to False for all dicts
    will be removed from the list.

    Example::
        from apollo.parsers import drop_empty_columns
        data = drop_empty_columns(data)
    """
    fieldnames = {field for doc in data for field in doc}
    for doc in data:
        for field in tuple(fieldnames):
            if doc.get(field):
                fieldnames.remove(field)
        if not fieldnames:
            break
    else:
        for doc in data:
            for field in fieldnames:
                if field in doc:
                    del doc[field]
    return data


def find_all_urls(text: str) -> list[str]:
    """Finds all urls in a string and returns a list"""
    url_regex = compile(
        "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|:%[0-9a-fA-F][0-9a-fA-F])+"
    )
    return findall(url_regex, text)


def reverse_geocode(x: float, y: float) -> dict[str, Any]:
    """Returns address from x and y coordinates using ArcGIS; reverse geocoding."""
    response = get(
        "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer"
        f"/reverseGeocode?location={x},{y}&f=json"
    )
    assert isinstance(response, Response)
    return response.json()


def partition(pred: Callable[[T], bool], it: Iterable[T]) -> tuple[list[T], list[T]]:
    """Split an iterable `it` into two lists using some predicate `pred`.

    This function is faster than using alternatives from stdlib, functools, or itertools!

    Example:
        from apollo.parsers import partition

        def is_even(num):
            return (num % 2) == 0

        data = range(100)
        evens, odds = partition(is_even, data)
    """
    ts: list[T] = []
    fs: list[T] = []
    t = ts.append
    f = fs.append
    for item in it:
        (t if pred(item) else f)(item)
    return ts, fs


def count_bytes(num: int) -> int:
    """Calculate the number of bytes needed to store `num` as a signed integer.

    Example:
        from apollo.parsers import count_bytes
        print(count_bytes(127), count_bytes(128))
    """
    num = int(num)
    return len(
        num.to_bytes((8 + (num + (num < 0)).bit_length()) // 8, "big", signed=True)
    )
