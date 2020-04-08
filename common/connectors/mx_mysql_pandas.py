from common import MySQLClient
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, DateTime, Text
from pandas import DataFrame, read_sql
from datetime import datetime
from numpy import ceil
from tqdm import tqdm
from typing import List
from functools import partial


class PandasSQL(MySQLClient):
    def __init__(self, database: str = None, table: str = None, **kwargs):
        super().__init__(database=database, table=table, **kwargs)
        config = self.__dict__['_MySQLClient__config']
        self.engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}",
            connect_args={k: v for k, v in config.items() if k in ['ssl_ca', 'ssl_cert', 'ssl_key']})
        self.psql_query = None
        self.psql_count = None
        self.psql_chunk_total = None
        self.df = None
        self.dtypes = {'Int64': Integer, 'int64': Integer, 'float64': Float(asdecimal=True),
                       datetime: DateTime, 'object': Text}

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
        self.psql_query = q if q else self.build()
        if limit:
            self.psql_query += f" LIMIT {limit}"
        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)

        if use_tqdm and chunk_size:
            self.psql_count = int(self.psql_query.upper().split('LIMIT')[-1]) \
                if 'LIMIT' in self.psql_query.upper() else self.psql_count()
            self.psql_chunk_total = int(ceil(self.psql_count / chunk_size))

        self.df = read_sql(
            sql=self.psql_query,
            con=self.engine,
            chunksize=chunk_size,
            columns=columns,
            index_col=index_columns
        )
        self.df = self.df if not use_tqdm else _tqdm(self.df)
        return self

    def to_sql(self, df: DataFrame = None, database: str = None, method: str = 'append'):
        database = self.database if not database else database
        df = self.df if not isinstance(df, DataFrame) else df
        df = df.astype({k: 'Int64' for k in ['oppervlakte', 'volume', 'number_of_objects', 'parcel_surface',
                                             'energy_label_registered']})

        df.to_sql(name='test', schema=database, con=self.engine, if_exists=method,
                  dtype={k: self.dtypes[v] for k, v in df.dtypes.apply(lambda x: x.name).to_dict().items()})
        return self


if __name__ == '__main__':
    p_sql = PandasSQL(database='real_estate', table='real_estate')
    dataframe = p_sql.get_df(limit=10_000).df
    p_sql.to_sql(dataframe, database='avix', method='append')
