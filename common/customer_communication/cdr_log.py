from datetime import datetime as dt
import pandas as pd
from common.connectors import MySQLClient
import numpy as np

class CdrLogger:
    def __init__(self, filename):
        self.filename = filename
        self.data = pd.read_csv(self.filename)

    def clean(self):
        self.data.columns = self.data.columns = [x.strip(" ") for x in self.data.columns]
        self.data = self.data.replace({np.nan : None})
        self.data = self.data.to_dict("records")


        def hasDate(inputString):
            return all(char.isdigit() for char in inputString)

        if hasDate(self.filename[:8]) == False:
            self.filename = (
                (dt.now().strftime("%Y%m%d") + "_" + self.filename)
                .replace("(\..*)", "")
                .replace(".csv", "")
                .replace(".xls", "")
                .replace(".xlsx", "")
            )


    def insert_mysql(self):
        sql = MySQLClient(f"cdr_history.{self.filename}")
        sql.insert_new(None, self.data)


def cdrlog_exe(filename):
    cdl = CdrLogger(filename)
    cdl.clean()
    cdl.insert_mysql()