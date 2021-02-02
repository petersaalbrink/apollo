"""Module for FileTransfer with the Matrixian Group Platform.

This module contains one object that can be used for both uploads to
and downloads from the Platform.

.. py:class:: common.platform.FileTranfer(
       user_id: str = None,
       username: str = None,
       email: str = None,
       filename: str = None,
   )

   Upload and download files to/from the Matrixian Platform.
"""

from __future__ import annotations

__all__ = (
    "FileTransferFTP"
)

import binascii
from ftplib import FTP, all_errors
try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from pathlib import Path
from typing import List, Sequence, Union

from bson import ObjectId
from Crypto.Cipher import AES
from pymongo.errors import PyMongoError

from .connectors.mx_email import EmailClient
from .connectors.mx_mongo import MongoDB
from .exceptions import FileTransferError
from .env import getenv


class FileTransferFTP:
    """Upload and download files to/from the Matrixian Platform.

    Example usage::
        ft = FileTransfer(
            username="Data Team",
            filename="somefile.csv",
        )

        # Upload "somefile.csv"
        ft.transfer().notify()

        # Download "somefile.csv"
        ft.download(
            ft.list_files().pop()
        )

        # Use as FTP object
        with ft as ftp:
            print(ftp.nlst())
    """

    def __init__(
            self,
            username: str = None,
            filename: str = None,
            user_id: str = None,
            email: str = None,
            **kwargs,
    ):
        """Create a FileTransfer object to upload/download files.

        To be able to connect to an account, you need to provide a
        username, a user_id or a email.

        To be able to upload a file, you need to provide a filename.
        """

        # Check
        if not user_id and not username and not email:
            raise FileTransferError("Provide either a name, ID, or email for the user.")

        # Prepare parameters
        self.filename = filename
        self.key = getenv("MX_CRYPT_PASSWORD").encode()
        if not self.key:
            raise FileTransferError(
                "Missing environment variable 'MX_CRYPT_PASSWORD' (FTP password decrypt key)")
        host = kwargs.pop("host", "prod")
        if host == "prod":
            self.host = "production_api.user", "ftp.platform.matrixiangroup.com"
        elif host == "dev":
            self.host = "dev_api.user", "ftp.develop.platform.matrixiangroup.com"
        else:
            raise FileTransferError("Host should be 'prod' or 'dev'.")

        # Connect to MongoDB
        self.db = MongoDB(self.host[0])
        self.test_mongo()

        # Find user details
        if user_id:
            q = {"_id": ObjectId(user_id)}
        elif username:
            q = {"username": username}
        elif email:
            q = {"email": email}
        else:
            raise FileTransferError("Provide either a name, ID or email for the user.")
        doc = self.db.find_one(q)
        if not doc:
            raise FileTransferError("User not found.")
        self.user_id = doc["_id"]
        self.username = doc["username"]
        self.email = doc["email"]
        self.encrypted_ftp_password = doc["ftpPassword"]

        self.ftp = FTP()

    def __enter__(self) -> FTP:
        self.connect()
        return self.ftp

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.disconnect()
        if any((exc_type, exc_val, exc_tb)):
            return False
        return True

    @cached_property
    def ftp_password(self) -> str:
        iv, ciphertext = map(binascii.a2b_hex, self.encrypted_ftp_password.split(":"))
        return AES.new(self.key, AES.MODE_CBC, iv).decrypt(ciphertext).strip().decode()

    def connect(self):
        self.ftp.connect(host=self.host[1], port=2121)
        self.ftp.login(user=self.email, passwd=self.ftp_password)

    def disconnect(self):
        self.ftp.quit()

    @cached_property
    def insert_filename(self) -> str:
        """Provide the file name needed for uploads."""
        return Path(self.filename).name

    def _check_filename(self):
        """Check if a filename is provided."""
        if not self.filename:
            raise FileTransferError("Provide a filename.")

    def list_files(self, _connect: bool = True) -> List[str]:
        """List existing files in this user's Platform folder."""
        if _connect:
            self.connect()
        data = self.ftp.nlst()
        if _connect:
            self.disconnect()
        return data

    def download(self, file: str, _connect: bool = True) -> FileTransferFTP:
        """Download an existing Platform file to disk."""
        if _connect:
            self.connect()
        with open(file, "wb") as f:
            self.ftp.retrbinary(f"RETR {file}", f.write)
        if _connect:
            self.disconnect()
        return self

    def download_all(self) -> FileTransferFTP:
        """Download all existing Platform files to disk."""
        self.connect()
        for file in self.list_files(_connect=False):
            self.download(file, _connect=False)
        self.disconnect()
        return self

    def upload(self) -> FileTransferFTP:
        """Upload a file to the Platform host."""
        self._check_filename()
        self.connect()
        with open(self.filename, "rb") as f:
            self.ftp.storbinary(f"STOR {self.filename}", f)
        self.disconnect()
        return self

    def test_mongo(self):
        """Test if a connection can be made to the MongoDB host."""
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise FileTransferError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    def test_ftp(self):
        """Test if a connection can be made to the Platform host."""
        try:
            self.connect()
            self.disconnect()
        except all_errors as e:
            raise FileTransferError(f"Make sure you have access to the FTP server.") from e

    def transfer(self) -> FileTransferFTP:
        """Upload the specified file to the specified Platform folder."""
        self.upload()
        return self

    def notify(
            self,
            to_address: Union[str, Sequence[str]] = None,
            username: str = None
    ) -> FileTransferFTP:
        """Notify the user of the new Platform upload."""

        # Prepare message
        template = Path(__file__).parent / "etc/email.html"
        with open(template) as f:
            message = f.read()
        message = message.replace("USERNAME", username or self.username)
        message = message.replace("FILENAME", self.insert_filename)

        # Prepare receivers
        if isinstance(to_address, str):
            to_address = (to_address, self.email)
        elif isinstance(to_address, Sequence):
            to_address = (*to_address, self.email)
        else:
            to_address = self.email

        # Send email
        EmailClient().send_email(
            to_address=to_address,
            subject="Nieuw bestand in Filetransfer",
            message=message,
        )

        return self
