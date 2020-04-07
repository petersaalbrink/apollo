from common import MySQLClient
from sqlalchemy import create_engine
from pandas import DataFrame, read_sql
from numpy import ceil
from tqdm import tqdm
from typing import List
from functools import partial


class PandasSQL(MySQLClient):
    def __init__(self, database: str = None, table: str = None, **kwargs):
        super().__init__(**kwargs)
        self.database = database
        self.table = table
        self.sql = MySQLClient(database=database, table=table)
        config = self.sql.__dict__['_MySQLClient__config']
        self.engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}",
            connect_args={k: v for k, v in config.items() if k in ['ssl_ca', 'ssl_cert', 'ssl_key']})
        self.query = None
        self.count = None
        self.chunk_total = None
        self.df = None

    def get_df(self, q: str = None, chunk_size: int = None, columns: List[str] = None,
               index_columns: List[str] = None, use_tqdm: bool = False, limit: int = None):
        """
        This function retrieves the data from MySQL and stores it into a DataFrame generator.

        Example 1:
            1.  p_sql = MySQLClient(database='real_estate', table='real_estate')
            2.  df = get_df()

        Example 2:
            1.  p_sql = MySQLClient(database='real_estate', table='real_estate')
            2.  for chunk in get_df(chunk_size=100_000, use_tqdm=True):
                    print(chunk)
        :param q:
        :param chunk_size:
        :param columns:
        :param index_columns:
        :param use_tqdm:
        :param limit:
        :return:
        """
        self.query = q if q else self.sql.build()
        if limit:
            self.query += f" LIMIT {limit}"
        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)

        if use_tqdm and chunk_size:
            self.count = int(self.query.upper().split('LIMIT')[-1]) \
                if 'LIMIT' in self.query.upper() else self.sql.count()
            self.chunk_total = int(ceil(self.count / chunk_size))

        self.df = read_sql(
            sql=self.query,
            con=self.engine,
            chunksize=chunk_size,
            columns=columns,
            index_col=index_columns
        )
        self.df = self.df if not use_tqdm else _tqdm(self.df)
        return self

    def to_sql(self, df: DataFrame = None, database: str = None, method: str = 'append'):
        database = self.database if not database else database
        df = self.df if not df else df
        df.to_sql(name='test', schema=database, con=self.engine, if_exists=method)
        return self


if __name__ == '__main__':
    p_sql = PandasSQL(database='real_estate', table='real_estate')
    dataframe = p_sql.get_df(limit=10_000).df
    p_sql.to_sql(dataframe, database='avix', method='append')
