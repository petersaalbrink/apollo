"""This module contains database connectors for Matrixian Group.

Connect to MySQL with MySQLClient
Connect to MongoDB with MongoDB
Connect to Elasticsearch with ESClient

There is also an EmailClient, which can be used to send emails.

There are two alternative connectors for MySQL: PandasSQL and SQLClient

Finally, there is a SQLtoMongo class for moving data from MySQL to MongoDB
"""
from .mx_elastic import ESClient
from .mx_email import EmailClient
from .mx_mongo import MongoDB
from .mx_mysql import MySQLClient
from .mx_pandassql import PandasSQL
from .mx_sqlalchemy import SQLClient
from .mx_sqltomongo import SQLtoMongo
