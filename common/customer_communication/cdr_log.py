from datetime import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from common.connectors import MySQLClient


def _add_date_to_filename(filename):
    return f"{dt.now():%Y%m%d}_{filename}"


class CdrLogger:
    def __init__(self, filename, delimiter=None, encoding=None):
        self.filename = filename
        self.data = pd.read_csv(self.filename, delimiter=delimiter, encoding=encoding, low_memory=False)

    def clean(self):
        self.data.columns = self.data.columns = [x.strip(" ") for x in self.data.columns]
        self.data = self.data.replace({np.nan: None})
        self.data = self.data.to_dict("records")

        def has_date(input_string):
            return all(char.isdigit() for char in input_string)

        if not has_date(self.filename[:8]):
            self.filename = _add_date_to_filename(self.filename)
        if "." in self.filename:
            raise ValueError(f"File name should not contain dot: '{self.filename}'")

    def insert_mysql(self):
        sql = MySQLClient("cdr_history")
        fields = sql.create_definition(data=self.data)
        sql.table_name = Path(self.filename).stem
        sql.create_table(table=sql.table_name, fields=fields, drop_existing=True, raise_on_error=True)
        sql.insert(data=self.data)


def cdrlog_exe(filename, delimiter=None, encoding=None):
    cdl = CdrLogger(filename, delimiter=delimiter, encoding=encoding)
    cdl.clean()
    cdl.insert_mysql()


def cdr_log(filename: str, data: list) -> int:
    filename = Path(_add_date_to_filename(filename)).stem
    if "." in filename:
        raise ValueError(f"File name should not contain dot: '{filename}'")
    return MySQLClient(f"cdr_history.{filename}").insert_new(data=data)
