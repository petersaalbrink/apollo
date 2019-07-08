import pathlib
import smtplib
import mysql.connector
from os import PathLike
from email import encoders
import mysql.connector.cursor
from typing import List, Union
from pymongo import MongoClient
from urllib.parse import quote_plus
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pymongo.database import Database
from pymongo.database import Collection
from elasticsearch import Elasticsearch
from email.mime.multipart import MIMEMultipart
from common.secrets import b64decode, es_pass, mail_pass, mongo_pass, sql_pass


class ElasticSearch(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self, dev: bool = True):
        if dev:
            host = '136.144.173.2'
        else:
            host = '37.97.169.90'
        super(ElasticSearch, self).__init__([{
            'host': host, 'port': 9201,
            'http_auth': ("psaalbrink@matrixiangroup.com", b64decode(es_pass).decode())}])


class EmailClient:
    """Client for sending plain text emails and attachments."""

    def __init__(self,
                 smtp_server='smtp.gmail.com:587',
                 login='dev@matrixiangroup.com',
                 password=b64decode(mail_pass).decode()):
        """Client for sending plain text emails and attachments."""
        self.smtp_server = smtp_server
        self.login = login
        self.password = password

    def send_email(self,
                   to_address: Union[str, list] = 'psaalbrink@matrixiangroup.com',
                   subject: str = 'no subject',
                   message: str = '',
                   from_address: str = 'dev@matrixiangroup.com',
                   attachment_path: Union[str, pathlib.Path, PathLike] = None,
                   ):
        """Send an email to an email address (str) or a list of addresses.
        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory."""

        msg = MIMEMultipart()
        msg['From'] = from_address
        if isinstance(to_address, str):
            msg['To'] = to_address
        elif isinstance(to_address, list):
            msg['To'] = ','.join(to_address)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        if attachment_path is not None:
            attachment = open(attachment_path, 'rb')
            p = MIMEBase('application', 'octet-stream')
            p.set_payload(attachment.read())
            encoders.encode_base64(p)
            if not isinstance(attachment_path, str):
                p.add_header('Content-Disposition', f"attachment; filename={attachment_path.name}")
            else:
                p.add_header('Content-Disposition', f"attachment; filename={attachment_path}")
            msg.attach(p)

        server = smtplib.SMTP(self.smtp_server)
        server.starttls()
        server.login(self.login, self.password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()


class MongoDB:
    """Client for MongoDB

    Usage:
        client = MongoDB()
        db = MongoDB('dev_peter')
        coll = MongoDB('dev_peter', 'person_data_20190606')
        coll = MongoDB()['dev_peter']['person_data_20190606']
    """

    def __new__(cls, database: str = None, collection: str = None) -> Union[MongoClient, Database, Collection]:
        """Client for MongoDB

        Usage:
            client = MongoDB()
            db = MongoDB('dev_peter')
            coll = MongoDB('dev_peter', 'person_data_20190606')
            coll = MongoDB()['dev_peter']['person_data_20190606']
        """
        user = 'devpsaalbrink'
        password = b64decode(mongo_pass).decode()
        host = '136.144.173.2'
        mongo_client = MongoClient(f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}")
        if database is not None:
            if collection is not None:
                return mongo_client.__getattr__(database).__getattr__(collection)
            return mongo_client.__getattr__(database)
        return mongo_client


class MySQLClient:
    """Client for MySQL"""
    config = {
        'user': 'trainee_peter',
        'password': b64decode(sql_pass).decode(),
        'host': '104.199.69.152',
        'database': 'mx_traineeship_peter',
        'raise_on_warnings': True,
        'client_flags': [mysql.connector.ClientFlag.SSL],
        'ssl_ca': pathlib.Path.home() / 'Python/common/certificates/server-ca.pem',
        'ssl_cert': pathlib.Path.home() / 'Python/common/certificates/client-cert.pem',
        'ssl_key': pathlib.Path.home() / 'Python/common/certificates/client-key.pem'}
    cnx = cursor = None

    def connect(self, conn: bool = False) -> mysql.connector.cursor.MySQLCursorBuffered:
        """Connect to SQL server"""
        self.cnx = mysql.connector.connect(**self.config)
        self.cnx.autocommit = True
        self.cursor = self.cnx.cursor(buffered=True)
        if conn:
            return self.cnx
        else:
            return self.cursor

    def disconnect(self):
        """Disconnect from SQL server"""
        self.cursor.close()
        self.cnx.close()

    def execute(self, query: str):
        self.cursor.execute(query)

    def executemany(self, query: str, data: list):
        self.connect()
        self.cursor.executemany(query, data)
        self.disconnect()

    def fetchall(self) -> List[tuple]:
        return self.cursor.fetchall()

    def column(self, query: str = None) -> List[str]:
        """Fetch a column from MySQL"""
        self.connect()
        if query is not None:
            self.cursor.execute(query)
        column = [value[0] for value in self.fetchall()]
        self.disconnect()
        return column

    def table(self, query: str = None) -> List[list]:
        """Fetch a table from MySQL"""
        self.connect()
        if query is not None:
            self.cursor.execute(query)
        table = [list(row) for row in self.fetchall()]
        self.disconnect()
        return table
