from datetime import datetime
from functools import cached_property
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
    """Upload a file to the Matrixian Platform.

    NB. There is no method yet to download files,
    but it can be made on request.

    Example usage::
        ft = FileTransfer(
            username="Data Team",
            filename="somefile.csv"
        )
        ft.transfer().notify()
    """

    def __init__(
            self,
            user_id: str = None,
            username: str = None,
            email: str = None,
            filename: str = None
    ):

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
        return f"{token_hex(10)}{round(time())}"

    @cached_property
    def insert_filename(self) -> str:
        return Path(self.filename).name

    @cached_property
    def filepath(self) -> str:
        return f"{self.ftpath}/{self.user_id}/{self.unique_dir}"

    @cached_property
    def cmds(self) -> Tuple[List[str], List[str], List[str]]:
        return (
            ["ssh", self.fthost, "install", "-d", "-m", "0777", self.filepath],
            ["scp", self.filename, f"{self.fthost}:{self.filepath}/"],
            ["ssh", self.fthost, "chmod", "777", f"{self.filepath}/{self.insert_filename}"],
        )

    @cached_property
    def doc(self):
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
        if not self.filename:
            raise FileTransferError("Provide a filename.")

    def _check_process(self, p):
        try:
            p.check_returncode()
        except CalledProcessError as e:
            raise FileTransferError(
                f"Error:\n{p.stdout.decode()}\n{p.stderr.decode()}\n"
                f"Make sure you have full access to user directory,"
                f" use command: `sudo chmod 777 {self.ftpath}` on {self.fthost}."
            ) from e

    def _run_cmd(self, cmd: List[str]):
        p = run(cmd, stdout=PIPE, stderr=PIPE)
        self._check_process(p)

    def list_files(self) -> list:
        """List existing files in this user's folder."""
        return [
            d["fileName"]
            for d in self.db.find({
                "owner": DBRef("user", ObjectId(self.user_id), "production_api")
            })]

    def download(self, file: str):
        """Download an existing file to disk."""
        result = self.db.find_one({"fileName": file})
        request = f'{self.fthost}:"{self.ftpath}/{result["filePath"]}/{result["fileName"]}"'
        cmd = ["scp", "-T", request, file]
        self._run_cmd(cmd)

    def download_all(self):
        """Download all existing files to disk."""
        for file in self.list_files():
            self.download(file)

    def filetransfer_database_entry(self) -> "FileTransfer":
        self._check_filename()
        self.db.insert_one(self.doc)
        return self

    def filetransfer_file_upload(self) -> "FileTransfer":
        self._check_filename()
        for cmd in self.cmds:
            self._run_cmd(cmd)
        return self

    def test_mongo(self):
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise FileTransferError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    def test_ssh(self):
        try:
            run(["ssh", self.fthost, "echo", "ssh", "ok"], check=True, stdout=DEVNULL)
        except CalledProcessError as e:
            raise FileTransferError(
                f"Make sure you have an entry for {self.fthost} in ~/.ssh/config."
            ) from e

    def test_connections(self):
        self.test_mongo()
        self.test_ssh()

    def transfer(self) -> "FileTransfer":
        self.filetransfer_file_upload()
        self.filetransfer_database_entry()
        return self

    def notify(
            self,
            to_address: Union[str, Sequence[str]] = None,
            username: str = None
    ) -> "FileTransfer":
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
