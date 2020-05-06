from mysql.connector import ClientFlag
from common.env import getenv, commondir
from common.secrets import get_secret
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import CHAR, DECIMAL, INTEGER
from pandas import DataFrame, read_sql
from numpy import ceil
from tqdm import tqdm
from typing import List
from functools import partial
from re import IGNORECASE, search
from typing import Sequence, Union


class PandasSQL:
    def __init__(self, database: str = None, table: str = None, **kwargs):
        self.database = database
        self.table = table
        envv = "MX_MYSQL_DEV_IP"
        usr, pwd = get_secret("MX_MYSQL_DEV")
        host = getenv(envv)
        self.__config = {
            "user": usr,
            "password": pwd,
            "host": host,
            "database": database,
            "raise_on_warnings": kwargs.get("raise_on_warnings", False),
            "client_flags": [ClientFlag.SSL]
        }
        self.__ssl = {
            "ssl_ca": f'{commondir / "server-ca.pem"}',
            "ssl_cert": f'{commondir / "client-cert.pem"}',
            "ssl_key": f'{commondir / "client-key.pem"}',
        }
        connection_string = f"mysql+mysqlconnector://{self.__config['user']}:{self.__config['password']}@" \
                            f"{self.__config['host']}/{self.__config['database']}"
        self.engine = create_engine(connection_string, connect_args=self.__ssl)
        self.session = sessionmaker(bind=self.engine)()
        self.psql_query = None
        self.psql_count = None
        self.psql_chunk_total = None
        self.df = None
        self.dtypes = {}

    def count(self) -> int:
        q = f"SELECT COUNT(*) FROM {self.database}.{self.table}"
        return self.session.execute(q).scalar()

    def get_dtypes(self) -> dict:
        md = MetaData()
        md.reflect(bind=self.engine)
        dtypes = {col.name: col.type for col in md.tables[self.table].c}
        return dtypes

    def set_dtypes(self, df, database, table) -> dict:
        # Create types from DataFrame
        for col in df.select_dtypes(include='float64'):
            lengths = df[col].where(lambda x: x.isna(), df[col].astype(str)).fillna('.') \
                .str.split('.', expand=True).applymap(lambda x: len(x)).max().to_list()
            _int = lengths[0]
            _frac = lengths[1]
            self.dtypes[col] = DECIMAL(precision=sum(lengths), scale=_frac)

        for col in df.select_dtypes(include=['int64', 'Int64']):
            display_width = df[col].where(lambda x: x.isna(), df[col].astype(str)) \
                .fillna('').astype(str).map(len).max()
            self.dtypes[col] = INTEGER(display_width=display_width)

        for col in df.select_dtypes(include='object'):
            self.dtypes[col] = CHAR(length=df[col].fillna('').str.len().max())

        # Check for column width of DataFrame and existing SQL table
        old_dtypes = self.get_dtypes()
        dtype_changes = {}
        if old_dtypes:
            for name, _type in self.dtypes.items():
                if 'DECIMAL' in str(_type) and 'DECIMAL' in str(old_dtypes[name]):
                    if _type.precision > old_dtypes[name].precision:
                        if _type.scale > old_dtypes[name].scale:
                            old_dtypes[name].scale = _type.scale
                        old_dtypes[name].precision = _type.precision
                        dtype_changes[name] = old_dtypes[name]
                if 'INT' in str(_type) and 'INT' in str(old_dtypes[name]):
                    if _type.display_width > old_dtypes[name].display_width:
                        old_dtypes[name].display_width = _type.display_width
                        dtype_changes[name] = old_dtypes[name]
                if 'CHAR' in str(_type) and 'CHAR' in str(old_dtypes[name]):
                    if _type.length > old_dtypes[name].length:
                        old_dtypes[name].length = _type.length
                        dtype_changes[name] = old_dtypes[name]

        # Modify column width if necessary
        if dtype_changes:
            query = f"ALTER TABLE {database}.{table} " \
                    f"MODIFY COLUMN {', MODIFY COLUMN '.join({f'{n} {t}' for n, t in dtype_changes.items()})}"
            self.engine.execute(query)

        return self.dtypes

    def get_df(self, query: str = None, chunk_size: int = None, columns: List[str] = None,
               index_columns: List[str] = None, use_tqdm: bool = False, limit: int = None,
               parse_dates: bool = None):
        """
        Description
        -----------
        This function retrieves the data from MySQL and returns a DataFrame. If a chunk_size is provided,
        the DataFrame is returned as an iterator.

        Parameters
        ----------
        query: str, default=None
            SQL query or a table name.
        chunk_size: int, default=None
            If specified, return an iterator where chunk_size is the number of rows to include in each chunk.
        columns: list, default=None
            List of column names to select from SQL table.
        index_columns: list, default=None
            Column(s) to set as index (MultiIndex).
        use_tqdm: bool, default=False
            Tracks the progress of the query if set to True.
        limit: int, default=None
            Adds a limit to the SQL query
        parse_dates: list or dict, default=None
            -   List of column names to parse as dates.
            -   Dict of {column_name: format string} where format string is strftime compatible in case of parsing
                string times, or is one of (D, s, ns, ms, us) in case of parsing integer timestamps.

        Returns
        -------
        A Pandas DataFrame or an iterator of a Pandas DataFrame

        Usage
        -----
        Example 1: Load data at once
            1.  p_sql = MySQLClient(database='real_estate', table='real_estate')
            2.  df = get_df().df

        Example 2: Load data in chunks
            1.  p_sql = MySQLClient(database='real_estate', table='real_estate')
            2.  for chunk in get_df(chunk_size=100_000, use_tqdm=True).df:
                    print(chunk)

        """
        self.psql_query = query if query else f"SELECT * FROM {self.database}.{self.table}"
        if limit:
            self.psql_query += f" LIMIT {limit}"

        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)

        if use_tqdm and chunk_size:
            self.psql_count = int(search(r'(?<=LIMIT\s)(\d+)', self.psql_query.upper(), IGNORECASE).group(1)) \
                if 'LIMIT' in self.psql_query.upper() else self.count()
            self.psql_chunk_total = int(ceil(self.psql_count / chunk_size))

        self.df = read_sql(
            sql=self.psql_query,
            con=self.engine,
            chunksize=chunk_size,
            columns=columns,
            index_col=index_columns,
            parse_dates=parse_dates
        )
        self.df = self.df if not use_tqdm else _tqdm(self.df)
        return self.df

    def to_sql(self, database: str, table: str, df: DataFrame = None, method: str = 'append',
               chunk_size: int = None, with_index: bool = False, index_label: Union[str, Sequence[str]] = None):
        """"
        Description
        -----------
        This function inserts a DataFrame to MySQL.

        Parameters
        ----------
        table: str
            Name of to be added table
        df: DataFrame, default=None
            Destination table
        database: str, default=None
            Destination database
        method: str, default='append'
            How to behave if the table already exists.
            1. fail: Raise a ValueError.
            2. replace: Drop the table before inserting new values.
            3. append: Insert new values to the existing table.
        chunk_size: int, default=None
            Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once
        with_index: bool, default=False
            Write DataFrame index as a column. Uses index_label as the column name in the table
        index_label: str or sequence, default None
            Column label for index column(s). If None is given (default) and index is True, then the index names are
            used. A sequence should be given if the DataFrame uses MultiIndex.

        Usage
        -----
        p_sql = PandasSQL(database=DATABASE_NAME, table=TABLE_NAME)
        p_sql.to_sql(database=DATABASE_NAME, table=TABLE_NAME, method='append', chunk_size=10_000, with_index=False)

        """
        df = self.df if not isinstance(df, DataFrame) else df
        dtypes = self.set_dtypes(df=df, database=database, table=table)

        # Replace NaN with None
        df = df.where(lambda x: x.notna(), None)

        df.to_sql(name=table, schema=database, con=self.engine, if_exists=method,
                  dtype=dtypes, method='multi', chunksize=chunk_size, index=with_index, index_label=index_label)
        return self


if __name__ == '__main__':
    p_sql = PandasSQL(database='real_estate', table='real_estate')
    dataframe = p_sql.get_df(limit=10_000)
    p_sql.to_sql(table='test', database='avix', method='append')

