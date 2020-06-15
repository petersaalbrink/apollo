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

from datetime import datetime
try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from pathlib import Path
from secrets import token_hex
from subprocess import run, CalledProcessError, DEVNULL, PIPE
from time import time
from typing import List, Sequence, Tuple, Union
from bson import DBRef, ObjectId
from pendulum import timezone
from pymongo.errors import PyMongoError
from .connectors.mx_email import EmailClient
from .connectors.mx_mongo import MongoDB
from .exceptions import FileTransferError


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
            user_id: str = None,
            username: str = None,
            email: str = None,
            filename: str = None,
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
        self.ftpath = f"/var/www/platform_projects_live_docker/public/upload/filetransfer"

        # Connect
        db = MongoDB("production_api.user", host="prod")
        self.db = MongoDB("production_api.filetransferFile", host="prod")
        self.test_connections()

        # Find user details
        if user_id:
            q = {"_id": ObjectId(user_id)}
        elif username:
            q = {"username": username}
        elif email:
            q = {"email": email}
        else:
            raise FileTransferError("Provide either a name, ID or email for the user.")
        doc = db.find_one(q)
        self.user_id = doc["_id"]
        self.username = doc["username"]
        self.email = doc["email"]

    @cached_property
    def unique_dir(self) -> str:
        """Provide the unique directory name needed for uploads."""
        return f"{token_hex(10)}{round(time())}"

    @cached_property
    def insert_filename(self) -> str:
        """Provide the file name needed for uploads."""
        return Path(self.filename).name

    @cached_property
    def filepath(self) -> str:
        """Provide the full directory path needed for uploads."""
        return f"{self.ftpath}/{self.user_id}/{self.unique_dir}"

    @cached_property
    def cmds(self) -> Tuple[List[str], List[str], List[str]]:
        """Provide the shell commands needed for uploads."""
        return (
            ["ssh", self.fthost, "install", "-d", "-m", "0777", self.filepath],
            ["scp", self.filename, f"{self.fthost}:{self.filepath}/"],
            ["ssh", self.fthost, "chmod", "777", f"{self.filepath}/{self.insert_filename}"],
        )

    @cached_property
    def doc(self):
        """Provide the MongoDB document needed for uploads."""
        return {
            "createdDate": datetime.now(tz=timezone("Europe/Amsterdam")),
            "filePath": f"{self.user_id}/{self.unique_dir}",
            "fileName": self.insert_filename,
            "fileSize": Path(self.filename).stat().st_size,
            "downloadsCount": 0,
            "creator": DBRef("user", ObjectId("5d8b7cee7704cf416b41ce93"), "production_api"),
            "owner": DBRef("user", ObjectId(self.user_id), "production_api")
        }

    def _check_filename(self):
        """Check if a filename is provided."""
        if not self.filename:
            raise FileTransferError("Provide a filename.")

    def _check_process(self, p):
        """Check if a process has completed successfully."""
        try:
            p.check_returncode()
        except CalledProcessError as e:
            raise FileTransferError(
                f"Error:\n{p.stdout.decode()}\n{p.stderr.decode()}\n"
                f"Make sure you have full access to user directory,"
                f" use command: `sudo chmod 777 {self.ftpath}` on {self.fthost}."
            ) from e

    def _run_cmd(self, cmd: List[str]):
        """Create a process from a shell command."""
        p = run(cmd, stdout=PIPE, stderr=PIPE)
        self._check_process(p)

    def list_files(self) -> list:
        """List existing files in this user's Platform folder."""
        return [
            d["fileName"]
            for d in self.db.find({
                "owner": DBRef("user", ObjectId(self.user_id), "production_api")
            })]

    def download(self, file: str):
        """Download an existing Platform file to disk."""
        result = self.db.find_one({"fileName": file})
        request = f'{self.fthost}:"{self.ftpath}/{result["filePath"]}/{result["fileName"]}"'
        cmd = ["scp", "-T", request, file]
        self._run_cmd(cmd)

    def download_all(self):
        """Download all existing Platform files to disk."""
        for file in self.list_files():
            self.download(file)

    def filetransfer_database_entry(self) -> "FileTransfer":
        """Create a MongoDB entry for the current upload."""
        self._check_filename()
        self.db.insert_one(self.doc)
        return self

    def filetransfer_file_upload(self) -> "FileTransfer":
        """Upload a file to the Platform host."""
        self._check_filename()
        for cmd in self.cmds:
            self._run_cmd(cmd)
        return self

    def test_mongo(self):
        """Test if a connection can be made to the MongoDB host."""
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise FileTransferError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    def test_ssh(self):
        """Test if a connection can be made to the Platform host."""
        try:
            run(["ssh", self.fthost, "echo", "ssh", "ok"], check=True, stdout=DEVNULL)
        except CalledProcessError as e:
            raise FileTransferError(
                f"Make sure you have an entry for {self.fthost} in ~/.ssh/config."
            ) from e

    def test_connections(self):
        """Test if connections can be made to the necessary hosts."""
        self.test_mongo()
        self.test_ssh()

    def transfer(self) -> "FileTransfer":
        """Upload the specified file to the specified Platform folder."""
        self.filetransfer_file_upload()
        self.filetransfer_database_entry()
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
