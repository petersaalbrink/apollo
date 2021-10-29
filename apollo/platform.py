"""Module for FileTransfer with the Matrixian Group Platform.

This module contains one object that can be used for both uploads to
and downloads from the Platform.

.. py:class:: apollo.platform.FileTranfer(
       user_id: str = None,
       username: str = None,
       email: str = None,
       filename: str = None,
   )

   Upload and download files to/from the Matrixian Platform.
"""

from __future__ import annotations

__all__ = (
    "FileTransfer",
    "FileTransferDocker",
    "FileTransferFTP",
    "SSHClient",
)

import binascii
import io
from collections.abc import Iterable
from datetime import datetime
from functools import cached_property
from logging import debug
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, NoReturn, Protocol

import paramiko
from bson import ObjectId
from Crypto.Cipher import AES
from pymongo.errors import PyMongoError

from .connectors.mx_email import EmailClient
from .connectors.mx_mongo import MongoDB, MxCollection
from .env import getenv
from .exceptions import FileTransferError
from .secrets import get_secret


class FileTransferSSHProtocol(Protocol):
    def __enter__(self) -> FileTransferSSHProtocol:
        ...

    def download(self) -> FileTransferSSHProtocol:
        ...

    def download_all(self) -> FileTransferSSHProtocol:
        ...

    def notify(self) -> FileTransferSSHProtocol:
        ...

    def transfer(self) -> FileTransferSSHProtocol:
        ...

    def upload(self) -> FileTransferSSHProtocol:
        ...


class FileTransferBase:
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
    """

    def __init__(
        self,
        username: str | None = None,
        filename: Path | str | None = None,
        user_id: str | None = None,
        email: str | None = None,
        host: str = "prod",
    ):
        """Create a FileTransfer object to upload/download files.

        To be able to connect to an account, you need to provide a
        username, a user_id or an email.

        To be able to upload a file, you need to provide a filename.
        """

        super().__init__()

        # Check
        if user_id:
            q = {"_id": ObjectId(user_id)}
        elif username:
            q = {"firstName": username}
        elif email:
            q = {"email": email}
        else:
            raise FileTransferError("Provide either a name, ID or email for the user.")

        # Prepare parameters
        self.filename = filename
        if host == "prod":
            self.user_collection = "production_api.user"
            self.ftp_host = "ftp.platform.matrixiangroup.com"
        elif host == "dev":
            self.user_collection = "dev_api.user"
            self.ftp_host = "ftp.develop.platform.matrixiangroup.com"
        else:
            raise FileTransferError("Host should be 'prod' or 'dev'.")

        # Connect to MongoDB
        self.db = MongoDB(self.user_collection)
        assert isinstance(self.db, MxCollection)

        # Lookup
        doc = self.db.find_one(q)
        if not doc:
            if username and username.count(" ") == 1:
                first, last = username.split()
                q = {"firstName": first, "lastName": last}
                doc = self.db.find_one(q)
            if not doc:
                raise FileTransferError("User not found.")

        # Set user detials
        self.user_id: ObjectId = doc["_id"]
        self.firstname: str = doc["firstName"]
        self.lastname: str = doc.get("lastName", "")
        self.email: str = doc["email"]
        self.encrypted_ftp_password: str = doc["ftpPassword"]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(email={self.email!r})"

    def check_filename(self) -> None:
        """Check if a filename is provided."""
        if not self.filename:
            raise FileTransferError("Provide a filename.")

    @cached_property
    def insert_filename(self) -> str:
        """Provide the file name needed for uploads."""
        assert isinstance(self.filename, (Path, str))
        return Path(self.filename).name

    def notify(
        self,
        to_address: str | list[str] | tuple[str, ...] | None = None,
        username: str | None = None,
    ) -> FileTransferBase:
        """Notify the user of the new Platform upload."""

        # Prepare message
        template = Path(__file__).parent / "etc/email.html"
        with open(template) as f:
            message = f.read()
        message = message.replace("USERNAME", username or self.firstname)
        message = message.replace("FILENAME", self.insert_filename)

        # Prepare receivers
        email = self.email.split("_unique_")[0]
        if isinstance(to_address, str):
            to_address = (to_address, email)
        elif isinstance(to_address, Iterable):
            to_address = (*to_address, email)
        else:
            to_address = email

        # Send email
        EmailClient().send_email(
            to_address=to_address,
            subject="Nieuw bestand in Filetransfer",
            message=message,
        )

        return self


class SSHClientMixin:
    client: paramiko.SSHClient
    filename: Path | str | None
    hostname: str
    port: int
    password: str
    username: str

    def __init__(self) -> None:
        self._connected = False
        self._filepath = Path()

    def __enter__(self) -> SSHClientMixin:
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.disconnect()
        if any(exc):
            return False
        return True

    def connect(self) -> None:
        if not self._connected:
            self.client.connect(
                hostname=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                timeout=10,
            )
            self._connected = True

    def disconnect(self) -> None:
        if self._connected:
            self.client.close()
            self._connected = False

    @staticmethod
    def check_process(stderr: paramiko.channel.ChannelFile) -> None:
        """Check if a process has completed successfully."""
        stderr_ = stderr.read().decode().strip()
        if stderr_:
            raise FileTransferError(stderr_)

    @property
    def filepath(self) -> Path:
        return self._filepath

    @filepath.setter
    def filepath(self, value: Path | str) -> None:
        if isinstance(value, Path):
            self._filepath = value
        elif isinstance(value, str):
            self._filepath = Path(value)
        else:
            raise TypeError(type(value))

    def download(self, file: str) -> None:
        """Download an existing Platform file to disk."""
        self.connect()
        remote_file = self.filepath / file
        stdout, stderr = self._run_cmd(f'sudo cat "{remote_file}"')
        with open(file, "wb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            while True:
                data = stdout.read(io.DEFAULT_BUFFER_SIZE)
                if not data:
                    break
                f.write(data)
                f.flush()
        self.check_process(stderr)
        self.disconnect()

    def download_all(self) -> None:
        """Download all existing Platform files to disk."""
        for file in self.list_files():
            self.download(file)

    def list_files(self) -> list[str]:
        """list existing files in this user's Platform folder."""
        self.connect()
        stdout, _ = self._run_cmd(f"sudo ls -t {self.filepath}")
        files = [f for f in stdout.read().decode().split("\n") if f]
        self.disconnect()
        return files

    def _cmd(
        self,
        cmd: str,
        fileobj: BinaryIO | None = None,
        sudo: bool = True,
    ) -> tuple[paramiko.channel.ChannelFile, paramiko.channel.ChannelFile]:

        debug("Executing command: %s", cmd)

        stdin, stdout, stderr = self.client.exec_command(
            cmd,
            bufsize=io.DEFAULT_BUFFER_SIZE,
            get_pty=sudo,
        )
        if sudo:
            stdin.write(f"{self.password}\n")
        stdin.flush()

        if fileobj:
            if fileobj.writable():
                while True:
                    data = stdout.read(io.DEFAULT_BUFFER_SIZE)
                    if not data:
                        break
                    fileobj.write(data)
                    fileobj.flush()
            else:
                while True:
                    data = fileobj.read(io.DEFAULT_BUFFER_SIZE)
                    if not data:
                        break
                    stdin.write(data)
                    stdin.flush()

        stdin.close()

        return stdout, stderr

    def exec_command(
        self,
        command: str,
        fileobj: BinaryIO | None = None,
        sudo: bool = False,
    ) -> str:
        self.connect()

        stdout, stderr = self._cmd(
            cmd=command,
            fileobj=fileobj,
            sudo=sudo,
        )

        self.check_process(stderr)
        out = stdout.read().decode().strip()

        self.disconnect()
        return out

    def _run_cmd(
        self,
        cmd: str,
        fileobj: BinaryIO | None = None,
        sudo: bool = True,
    ) -> tuple[paramiko.channel.ChannelFile, paramiko.channel.ChannelFile]:
        """Create a process from a shell command."""
        stdout, stderr = self._cmd(
            cmd=cmd,
            fileobj=fileobj,
            sudo=sudo,
        )
        return stdout, stderr

    def transfer(self) -> SSHClientMixin:
        """Upload a file to the Platform host."""
        self.connect()
        assert isinstance(self.filename, (Path, str))
        local_filename = Path(self.filename)
        remote_filename = local_filename.name
        remote_file = self.filepath / remote_filename
        with open(local_filename, "rb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            _, stderr = self._run_cmd(
                f'sudo cp /dev/stdin "{remote_file}"',
                fileobj=f,
                sudo=True,
            )
        self.check_process(stderr)
        _, stderr = self._run_cmd(f'sudo chmod +r "{remote_file}"')
        self.check_process(stderr)
        self.disconnect()
        return self


def get_ssh_client() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    return client


class FileTransferDocker(FileTransferBase, SSHClientMixin):
    def __init__(
        self,
        username: str | None = None,
        filename: Path | str | None = None,
        user_id: str | None = None,
        email: str | None = None,
        host: str = "prod",
    ):
        super().__init__(
            username=username,
            filename=filename,
            user_id=user_id,
            email=email,
            host=host,
        )

        # Connect
        _, self.password = get_secret("MX_PLATFORM_HOST")
        self.client = get_ssh_client()

        self.fthost = "consucom"
        self.ftpath = Path("/var/lib/docker/volumes/filetransfer_live/_data")

        self.hostname = "136.144.203.100"
        self.port = 2233
        self.username = "consucom"

    def upload(self) -> NoReturn:
        raise NotImplementedError

    @property
    def filepath(self) -> Path:
        """Provide the full directory path needed for uploads."""
        return self.ftpath / f"{self.user_id}"

    @filepath.setter
    def filepath(self, value: Any) -> NoReturn:
        raise NotImplementedError

    def transfer(self) -> FileTransferDocker:
        self.check_filename()
        super().transfer()
        return self


class FileTransferFTP(FileTransferBase):
    def __init__(
        self,
        username: str | None = None,
        filename: Path | str | None = None,
        user_id: str | None = None,
        email: str | None = None,
        host: str = "prod",
    ):
        super().__init__(
            username=username,
            filename=filename,
            user_id=user_id,
            email=email,
            host=host,
        )

        self.key = getenv("MX_CRYPT_PASSWORD", "").encode()
        if not self.key:
            raise FileTransferError(
                "Missing environment variable 'MX_CRYPT_PASSWORD' (FTP password decrypt key)"
            )

        self.ftp: paramiko.SFTPClient | None = None

    def __enter__(self) -> paramiko.SFTPClient:
        self.connect()
        assert isinstance(self.ftp, paramiko.SFTPClient)
        return self.ftp

    def __exit__(
        self,
        exc_type: type[BaseException] | None,  # noqa
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.disconnect()
        if any((exc_type, exc_val, exc_tb)):
            return False
        return True

    @cached_property
    def ftp_password(self) -> str:
        iv, ciphertext = map(binascii.a2b_hex, self.encrypted_ftp_password.split(":"))
        return AES.new(self.key, AES.MODE_CBC, iv).decrypt(ciphertext).strip().decode()

    def connect(self) -> None:
        transport = paramiko.Transport((self.ftp_host, 2121))
        transport.connect(None, self.email, self.ftp_password)
        self.ftp = paramiko.SFTPClient.from_transport(transport)

    def disconnect(self) -> None:
        assert isinstance(self.ftp, paramiko.SFTPClient)
        self.ftp.close()

    def list_files(
        self,
        with_timestamp: bool = False,
        _connect: bool = True,
    ) -> list[str | tuple[str, datetime]]:
        """List existing files in this user's Platform folder."""
        if _connect:
            self.connect()
        assert isinstance(self.ftp, paramiko.SFTPClient)
        try:
            if with_timestamp:
                return [
                    (attr.filename, datetime.fromtimestamp(attr.st_mtime or 0))
                    for attr in sorted(
                        self.ftp.listdir_iter(),
                        key=lambda a: a.st_mtime or 0,
                        reverse=True,
                    )
                ]
            else:
                return [
                    attr.filename
                    for attr in sorted(
                        self.ftp.listdir_iter(),
                        key=lambda a: a.st_mtime or 0,
                        reverse=True,
                    )
                ]
        finally:
            if _connect:
                self.disconnect()

    def download(self, file: str, _connect: bool = True) -> FileTransferFTP:
        """Download an existing Platform file to disk."""
        if _connect:
            self.connect()
        assert isinstance(self.ftp, paramiko.SFTPClient)
        self.ftp.get(file, file)
        if _connect:
            self.disconnect()
        return self

    def download_all(self) -> FileTransferFTP:
        """Download all existing Platform files to disk."""
        self.connect()
        for file in self.list_files(_connect=False):
            assert isinstance(file, str)
            self.download(file, _connect=False)
        self.disconnect()
        return self

    def upload(self) -> FileTransferFTP:
        """Upload a file to the Platform host."""
        self.check_filename()
        self.connect()
        assert isinstance(self.ftp, paramiko.SFTPClient)
        if isinstance(self.filename, Path):
            filename = f"{self.filename}"
        elif isinstance(self.filename, bytes):
            filename = self.filename.decode()
        elif isinstance(self.filename, str):
            filename = self.filename
        else:
            raise TypeError(type(self.filename))
        self.ftp.put(filename, self.insert_filename)
        self.disconnect()
        return self

    def test_mongo(self) -> None:
        """Test if a connection can be made to the MongoDB host."""
        assert isinstance(self.db, MxCollection)
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise FileTransferError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    def test_ftp(self) -> None:
        """Test if a connection can be made to the Platform host."""
        try:
            self.connect()
            self.disconnect()
        except paramiko.SSHException as e:
            raise FileTransferError(
                "Make sure you have access to the FTP server."
            ) from e

    def transfer(self) -> FileTransferFTP:
        """Upload the specified file to the specified Platform folder."""
        self.upload()
        return self


class SSHClient(SSHClientMixin):
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        port: int = 2233,
    ):
        super().__init__()
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.client = get_ssh_client()

    def notify(self) -> NoReturn:
        raise NotImplementedError

    def upload(self) -> NoReturn:
        raise NotImplementedError

    def __enter__(self) -> SSHClient:
        super().__enter__()
        return self

    def __exit__(self, *exc: Any) -> bool:
        return super().__exit__(*exc)


FileTransfer = FileTransferFTP
