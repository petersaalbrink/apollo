from common import MySQLClient

from pandas import read_sql
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
        self.query = None
        self.count = None

    def get_df(self, query: str = None, chunk_size: int = None, columns: List[str] = None,
               index_columns: List[str] = None, use_tqdm: bool = False):
        """
        This function retrieves the data from MySQL and stores it into a DataFrame generator.

        Example:
            1.  p_sql = MySQLClient(database='real_estate', table='real_estate')
            2.  for chunk in get_df(chunk_size=100_000, use_tqdm=True):

        :param query:
        :param chunk_size:
        :param columns:
        :param index_columns:
        :param use_tqdm:
        :return:
        """
        self.query = query if query else self.sql.build()
        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)

        if use_tqdm:
            self.count = self.sql.count()

        self.sql.connect()
        generator_df = _tqdm(read_sql(
            sql=self.query,
            con=self.sql.cnx,
            chunksize=chunk_size,
            columns=columns,
            index_col=index_columns
        ), total=int(ceil(self.count / chunk_size)) if use_tqdm else self.count)
        return generator_df


if __name__ == '__main__':
    p_sql = PandasSQL(database='real_estate', table='real_estate')
    query = """
        SELECT 
            re.address_id, 
            transactie_datum AS last_date, 
            CASE
                WHEN YEAR(CURDATE()) - YEAR(transactie_datum) <= 5 THEN TRUE
                ELSE FALSE
            END AS transaction_is_young, 
            koopsom AS last_amount, 
            indexed_transaction, 
            CASE
                WHEN indexed_transaction IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS transaction_exists,
            nnp_verkrijger, 
            nnp_verwerker,
            meer_ontr_goed,
            postcode_median,
            postcode_count,
            postcode_sametype_median,
            postcode_sametype_count,
            street_median,
            street_count,
            street_sametype_median,
            street_sametype_count,
            indexed_transaction_pc4_count,
            indexed_transaction_pc4_mean,
            dev_tot,
            score_tot,
            CASE 
                WHEN oppervlakte < 50 THEN '0-50'
                WHEN oppervlakte < 100 THEN '50-100'
                WHEN oppervlakte < 150 THEN '100-150'
                WHEN oppervlakte < 250 THEN '150-250'
                WHEN oppervlakte < 500 THEN '250-500'
                WHEN oppervlakte < 1000 THEN '500-1000'
                WHEN oppervlakte < 10000 THEN '1000-10000'
                WHEN oppervlakte IS NULL THEN NULL
                ELSE '10000+'
            END AS use_surface_category,
            CASE 
                WHEN build_year < 1800 THEN '-1800'
                WHEN build_year < 1905 THEN '1800-1905'
                WHEN build_year < 1925 THEN '1905-1925'
                WHEN build_year < 1945 THEN '1925-1945'
                WHEN build_year < 1970 THEN '1945-1970'
                WHEN build_year < 1980 THEN '1970-1980'
                WHEN build_year < 1990 THEN '1980-1990'
                WHEN build_year < 2000 THEN '1990-2000'
                WHEN build_year < 2010 THEN '2000-2010'
                WHEN build_year IS NULL THEN NULL
                ELSE '2010+'
            END AS build_year_category
        FROM (
            SELECT 
                gemeentecode,
                postcode,
                straatnaam,
                CONCAT_WS(' ', postcode, huisnr, 
                    CONCAT_WS('', huisnr_bag_letter, huisnr_bag_toevoeging)
                ) AS address_id, 
                build_type,
                oppervlakte,
                build_year
            FROM real_estate.real_estate
        ) AS re
        LEFT JOIN (
            SELECT 
                address_id,
                transactie_datum,
                koopsom,
                indexed_transaction,
                nnp_verkrijger,
                nnp_verwerker,
                meer_ontr_goed
            FROM indexed_last_transactions
        ) AS t
        ON re.address_id = t.address_id
        LEFT JOIN (
            SELECT 
                postcode, 
                build_type, 
                postcode_sametype_median, 
                postcode_sametype_count, 
                postcode_median, 
                postcode_count
            FROM indexed_transactions_pc6
        ) AS pc
        ON re.postcode = pc.postcode AND re.build_type = pc.build_type
        LEFT JOIN (
            SELECT 
                gemeentecode, 
                straatnaam, 
                build_type, 
                street_sametype_median, 
                street_sametype_count, 
                street_median, 
                street_count
            FROM indexed_transactions_street
        ) AS s
        ON re.gemeentecode = s.gemeentecode AND re.straatnaam = s.straatnaam AND re.build_type = s.build_type
        LEFT JOIN (
            SELECT
                address_id,
                dev_tot,
                score_tot
            FROM
                mx_traineeship_jurgen.leefbaarheid_enriched
        ) AS leef
        ON re.address_id = leef.address_id
        LEFT JOIN (
            SELECT
                postcode4,
                count AS indexed_transaction_pc4_count,
                indexed_transaction AS indexed_transaction_pc4_mean
            FROM
                postcode_segmentation.indexed_transactions_pc4
        ) AS ps
        ON LEFT(re.address_id, 4) = ps.postcode4
        GROUP BY re.address_id
    """
    print(p_sql)
    import time
    start = time.time()
    for chunk in p_sql.get_df(query=query, chunk_size=100_000, use_tqdm=True):
        print(chunk)
        print(chunk.columns)
    print(f"{time.time()-start:.2f} seconds")

