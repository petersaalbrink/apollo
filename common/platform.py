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

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
import io
from pathlib import Path
from typing import BinaryIO, List, Sequence, Tuple, Union

from bson import ObjectId
from paramiko import AutoAddPolicy, SSHClient

from .connectors.mx_email import EmailClient
from .connectors.mx_mongo import MongoDB
from .exceptions import FileTransferError
from .secrets import get_secret


class FileTransfer:
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
            username: str = None,
            filename: str = None,
            user_id: str = None,
            email: str = None,
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
        self.fthost = "consucom"
        self.ftpath = f"/var/lib/docker/volumes/filetransfer_live/_data"

        # Find user details
        if user_id:
            q = {"_id": ObjectId(user_id)}
        elif username:
            q = {"username": username}
        elif email:
            q = {"email": email}
        else:
            raise FileTransferError("Provide either a name, ID or email for the user.")
        doc = MongoDB("production_api.user", host="prod").find_one(q)
        self.user_id = doc["_id"]
        self.username = doc["username"]
        self.email = doc["email"]

        # Connect
        self._usr, self._pwd = get_secret("MX_PLATFORM_HOST")
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(AutoAddPolicy())

    def _connect(self):
        self.client.connect(
            hostname="136.144.203.100",
            username="consucom",
            password=self._pwd,
            port=2233,
            timeout=10,
        )

    def _disconnect(self):
        self.client.close()

    @cached_property
    def insert_filename(self) -> str:
        """Provide the file name needed for uploads."""
        return Path(self.filename).name

    @cached_property
    def filepath(self) -> str:
        """Provide the full directory path needed for uploads."""
        return f"{self.ftpath}/{self.user_id}"

    def _check_filename(self):
        """Check if a filename is provided."""
        if not self.filename:
            raise FileTransferError("Provide a filename.")

    @staticmethod
    def _check_process(stderr: BinaryIO):
        """Check if a process has completed successfully."""
        stderr = stderr.read().decode()
        if stderr[:30] != "[sudo] password for consucom: " or stderr[30:]:
            raise FileTransferError(stderr)

    def _run_cmd(self, cmd: str, fileobj: BinaryIO = None) -> Tuple[BinaryIO, BinaryIO]:
        """Create a process from a shell command."""
        stdin, stdout, stderr = self.client.exec_command(
            f"sudo -S {cmd}",
            bufsize=io.DEFAULT_BUFFER_SIZE,
            timeout=10,
        )
        stdin.write(f"{self._pwd}\n")
        stdin.flush()
        if fileobj:
            while True:
                data = fileobj.read(io.DEFAULT_BUFFER_SIZE)
                if not data:
                    break
                stdin.write(data)
                stdin.flush()
        stdin.close()
        return stdout, stderr

    def list_files(self) -> List[str]:
        """List existing files in this user's Platform folder."""
        self._connect()
        stdout, _ = self._run_cmd(f"ls {self.filepath}")
        files = [f for f in stdout.read().decode().split("\n") if f]
        self._disconnect()
        return files

    def download(self, file: str):
        """Download an existing Platform file to disk."""
        self._connect()
        stdout, stderr = self._run_cmd(f'cat "{self.filepath}/{file}"')
        with open(file, "wb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            while True:
                data = stdout.read(io.DEFAULT_BUFFER_SIZE)
                if not data:
                    break
                f.write(data)
        self._check_process(stderr)
        self._disconnect()

    def download_all(self):
        """Download all existing Platform files to disk."""
        for file in self.list_files():
            self.download(file)

    def transfer(self) -> "FileTransfer":
        """Upload a file to the Platform host."""
        self._check_filename()
        self._connect()
        with open(self.filename, "rb", buffering=io.DEFAULT_BUFFER_SIZE) as f:
            _, stderr = self._run_cmd(
                f'cp /dev/stdin "{self.filepath}/{self.filename}"',
                fileobj=f,
            )
        self._check_process(stderr)
        _, stderr = self._run_cmd(f'chmod +r "{self.filepath}/{self.filename}"')
        self._check_process(stderr)
        self._disconnect()
        return self

    def notify(
            self,
            to_address: Union[str, Sequence[str]] = None,
            username: str = None
    ) -> "FileTransfer":
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
