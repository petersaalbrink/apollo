from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from common.connectors.mx_mysql import MySQLClient


def _add_date_to_filename(filename: str) -> str:
    return f"{dt.now():%Y%m%d}_{filename}"


def _has_date(input_string: str) -> bool:
    return all(char.isdigit() for char in input_string)


def _clean_filename(filename: str) -> str:
    if not _has_date(filename[:8]):
        filename = _add_date_to_filename(filename)
    filename = Path(filename).stem
    if "." in filename:
        raise ValueError(f"File name should not contain dot: '{filename}'")
    return filename


class CdrLogger:
    def __init__(
        self,
        filename: str,
        delimiter: str | None = None,
        encoding: str | None = None,
    ):
        self.filename = filename
        self.data = pd.read_csv(
            self.filename,
            delimiter=delimiter,
            encoding=encoding,
            low_memory=False,
        )

    def clean(self) -> None:
        self.data.columns = self.data.columns = [
            x.strip(" ") for x in self.data.columns
        ]
        self.data = self.data.replace({np.nan: None})
        self.data = self.data.to_dict("records")
        self.filename = _clean_filename(self.filename)

    def insert_mysql(self) -> None:
        sql = MySQLClient("cdr_history")
        fields = sql.create_definition(data=self.data)
        sql.table_name = Path(self.filename).stem
        sql.create_table(
            table=sql.table_name,
            fields=fields,
            drop_existing=True,
            raise_on_error=True,
        )
        sql.insert(data=self.data)


def cdrlog_exe(
    filename: str,
    delimiter: str | None = None,
    encoding: str | None = None,
) -> None:
    cdl = CdrLogger(
        filename=filename,
        delimiter=delimiter,
        encoding=encoding,
    )
    cdl.clean()
    cdl.insert_mysql()


def cdr_log(filename: str, data: Sequence[dict[str, Any]]) -> int:
    sql = MySQLClient(f"cdr_history.{_clean_filename(filename)}")
    return sql.insert_new(data=data)
