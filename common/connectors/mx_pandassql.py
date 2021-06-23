from __future__ import annotations

__all__ = ("PandasSQL",)

from collections.abc import Sequence
from functools import partial
from re import IGNORECASE, search
from typing import Any

from numpy import ceil, datetime64
from pandas import DataFrame, read_sql
from sqlalchemy.dialects.mysql import CHAR, DATE, DECIMAL, INTEGER
from tqdm import tqdm

from common.connectors.mx_sqlalchemy import SQLClient


class PandasSQL(SQLClient):
    """This class inherits SQLClient, such that it forms a connection between Pandas and
    Matrixian's MySQL database. It includes three functions:
        1. Getting the data from SQL into a DataFrame (`get_df`)
        2. Preparing the types of a DataFrame and SQL table before inserting. Note that this
           function (`set_dtypes`) also modifies the SQL table if the maximum column length of the
           DataFrame is larger than the length of the column in SQL.
        3. Inserting the DataFrame into SQL (`to_sql`)
    """

    def __init__(
        self,
        database: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(database=database, table=table, **kwargs)
        self.psql_count: int | None = None
        self.psql_chunk_total: int | None = None
        self.df: DataFrame | None = None
        self.dtypes: dict[str, Any] = {}

    def set_dtypes(
        self,
        df: DataFrame,
        database: str,
        table: str,
    ) -> dict[str, Any]:
        """This function sets the types for the DataFrame columns before uploading to SQL"""
        # Create types from DataFrame
        for col in df.select_dtypes(include=["float64", "Float64"]):
            _int, _frac = (
                df[col]
                .fillna(0.0)
                .astype(str)
                .str.split(".", expand=True)
                .applymap(len)
                .max()
                .to_list()
            )
            self.dtypes[col] = DECIMAL(precision=sum([_int, _frac]), scale=_frac)

        for col in df.select_dtypes(include=["int64", "Int64"]):
            display_width = df[col].fillna(0).astype(str).str.len().max()
            self.dtypes[col] = INTEGER(display_width=display_width)

        for col in df.select_dtypes(include=["object", "string"]):
            self.dtypes[col] = CHAR(length=df[col].fillna("").str.len().max())

        for col in df.select_dtypes(include=datetime64):
            self.dtypes[col] = DATE()

        # Check for column width of DataFrame and existing SQL table
        old_dtypes = self.get_dtypes()
        dtype_changes = {}
        if old_dtypes:
            for name, _type in self.dtypes.items():
                if "DECIMAL" in str(_type) and "DECIMAL" in str(old_dtypes[name]):
                    if _type.precision > old_dtypes[name].precision:
                        if _type.scale > old_dtypes[name].scale:
                            old_dtypes[name].scale = _type.scale
                        old_dtypes[name].precision = _type.precision
                        dtype_changes[name] = old_dtypes[name]
                if "INT" in str(_type) and "INT" in str(old_dtypes[name]):
                    if _type.display_width > old_dtypes[name].display_width:
                        old_dtypes[name].display_width = _type.display_width
                        dtype_changes[name] = old_dtypes[name]
                if "CHAR" in str(_type) and "CHAR" in str(old_dtypes[name]):
                    if _type.length > old_dtypes[name].length:
                        old_dtypes[name].length = _type.length
                        dtype_changes[name] = old_dtypes[name]

        # Modify column width if necessary
        if dtype_changes:
            modify = ", MODIFY COLUMN ".join(
                {f"{n} {t}" for n, t in dtype_changes.items()}
            )
            query = f"ALTER TABLE {database}.{table} MODIFY COLUMN {modify}"
            self.engine.execute(query)

        return self.dtypes

    def get_df(
        self,
        query: str | None = None,
        chunk_size: int | None = None,
        columns: list[str] | None = None,
        index_columns: list[str] | None = None,
        use_tqdm: bool = False,
        limit: int | None = None,
        parse_dates: bool | list[str] | dict[str, str] | None = None,
    ) -> DataFrame:
        """
        Description
        -----------
        This function retrieves the data from MySQL and returns a DataFrame. If a chunk_size is
        provided, the DataFrame is returned as an iterator.

        Parameters
        ----------
        query: str, default=None
            SQL query or a table name.
        chunk_size: int, default=None
            If specified, return an iterator where chunk_size is the number of rows to include in
            each chunk.
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
            -   Dict of {column_name: format string} where format string is strftime compatible in
                case of parsing string times, or is one of (D, s, ns, ms, us) in case of parsing
                integer timestamps.

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

        if query and limit:
            query += f" LIMIT {limit}"

        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)

        if use_tqdm and chunk_size:
            if query and "LIMIT" in query.upper():
                match = search(r"(?<=LIMIT\s)(\d+)", query.upper(), IGNORECASE)
                if match:
                    self.psql_count = int(match.group(1))
            self.psql_count = self.count() if not self.psql_count else self.psql_count
            assert isinstance(self.psql_count, int)
            self.psql_chunk_total = int(ceil(self.psql_count / chunk_size))

        self.df = read_sql(
            sql=query if query else self.table,
            con=self.engine,
            chunksize=chunk_size,
            columns=columns,
            index_col=index_columns,
            parse_dates=parse_dates,
        )
        self.df = self.df if not use_tqdm else _tqdm(self.df)
        return self.df

    def to_sql(
        self,
        database: str,
        table: str,
        df: DataFrame | None = None,
        method: str = "append",
        chunk_size: int | None = None,
        with_index: bool = False,
        index_label: str | Sequence[str] | None = None,
    ) -> PandasSQL:
        """ "
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
            Column label for index column(s). If None is given (default) and index is True, then
            the index names are used. A sequence should be given if the DataFrame uses MultiIndex.

        Example
        -------
        p_sql = PandasSQL(database=DATABASE_NAME, table=TABLE_NAME)
        p_sql.to_sql(
            database=DATABASE_NAME,
            table=TABLE_NAME,
            method='append',
            chunk_size=10_000,
            with_index=False,
        )
        """
        df = self.df if not isinstance(df, DataFrame) else df
        assert isinstance(df, DataFrame)

        # Set dtypes
        dtypes = self.set_dtypes(df=df, database=database, table=table)

        # Replace NaN with None
        df = df.where(lambda x: x.notna(), None)

        df.to_sql(
            name=table,
            schema=database,
            con=self.engine,
            if_exists=method,
            dtype=dtypes,
            method="multi",
            chunksize=chunk_size,
            index=with_index,
            index_label=index_label,
        )
        return self
