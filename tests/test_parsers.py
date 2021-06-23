from __future__ import annotations

from datetime import datetime
from string import ascii_letters
from typing import Any

import hypothesis.strategies as st
from hypothesis import given

from common.parsers import dateformat, flatten, levenshtein

strs = st.text(ascii_letters, min_size=2)
json = st.recursive(
    st.dictionaries(strs, strs, min_size=1),
    lambda children: st.dictionaries(strs, children, min_size=1),
)


@given(
    d=st.integers(1, 31),
    m=st.integers(1, 12),
    y=st.integers(1970, 2050),
    s=st.sampled_from("/-"),
    year_first=st.booleans(),
)
def test_dateformat(d: int, m: int, y: int, s: str, year_first: bool) -> None:
    date_string = (
        f"{y}{s}{m:02d}{s}{d:02d}" if year_first else f"{d:02d}{s}{m:02d}{s}{y}"
    )
    try:
        fmt = dateformat(date_string)
        parsed = datetime.strptime(date_string, fmt).date()
        assert date_string == parsed.strftime(fmt)
    except ValueError as e:
        assert f"{e}".startswith("day is out of range for month")


@given(nested_dict=json)
def test_flatten(nested_dict: dict[str, dict[str, Any]]) -> None:
    flattened = flatten(nested_dict)
    assert all(not isinstance(v, dict) for v in flattened.values())


@given(seq1=strs)
def test_levenshtein(seq1: str) -> None:
    seq2 = f"{chr(ord(seq1[0]) + 1)}{seq1[1:]}"
    assert 0.5 <= levenshtein(seq1, seq2, "percentage") <= 1
    assert levenshtein(seq1, seq2, "distance") == 1
