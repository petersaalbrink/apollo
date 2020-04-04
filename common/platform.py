from datetime import datetime
from pathlib import Path
from secrets import token_hex
from subprocess import run, CalledProcessError, DEVNULL, PIPE
from time import time
from typing import Sequence, Union
from bson import DBRef, ObjectId
from pendulum import timezone
from pymongo.errors import PyMongoError
from .connectors.email import EmailClient
from .connectors.mongodb import MongoDB


class FileTransferError(Exception):
    pass


class FileTransfer:
    def __init__(self,
                 user_id: str = None,
                 username: str = None,
                 email: str = None,
                 filename: str = None):

        # Check
        if sum((user_id is not None, username is not None, email is not None)) != 1:
            raise ValueError("Provide either a name, ID, or email for the user.")
        if filename is None:
            raise ValueError("Provide a filename.")

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
            raise ValueError("Provide either a name, ID or email for the user.")
        doc = db.find_one(q)
        self.user_id = doc["_id"]
        self.username = doc["username"]
        self.email = doc["email"]

        # Prepare parameters
        self.filename = filename
        self.insert_filename = Path(filename).name
        self.ftpath = f"/var/www/platform_projects_live_docker/public/upload/filetransfer"
        self.unique_dir = f"{token_hex(10)}{round(time())}"
        self.filepath = f"{self.ftpath}/{self.user_id}/{self.unique_dir}"
        self.cmds = (
            ["ssh", "consucom", "install", "-d", "-m", "0777", self.filepath],
            ["scp", self.filename, f"consucom:{self.filepath}/"],
            ["ssh", "consucom", "chmod", "777", f"{self.filepath}/{self.insert_filename}"],
        )
        self.doc = {
            "createdDate": datetime.now(tz=timezone("Europe/Amsterdam")),
            "filePath": f"{self.user_id}/{self.unique_dir}",
            "fileName": self.insert_filename,
            "fileSize": Path(self.filename).stat().st_size,
            "downloadsCount": 0,
            "creator": DBRef("user", ObjectId("5d8b7cee7704cf416b41ce93"), "production_api"),
            "owner": DBRef("user", ObjectId(self.user_id), "production_api")
        }

    def filetransfer_database_entry(self) -> "FileTransfer":
        self.db.insert_one(self.doc)
        return self

    def filetransfer_file_upload(self) -> "FileTransfer":
        for cmd in self.cmds:
            p = run(cmd, stdout=PIPE, stderr=PIPE)
            try:
                p.check_returncode()
            except CalledProcessError as e:
                raise FileTransferError(
                    f"Error:\n{p.stdout.decode()}\n{p.stderr.decode()}\n"
                    f"Make sure you have full access to user directory,"
                    f" use command: `sudo chmod 777 {self.ftpath}` on consucom."
                ) from e
        return self

    def test_mongo(self):
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise FileTransferError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    @staticmethod
    def test_ssh():
        try:
            run(["ssh", "consucom", "echo", "ssh", "ok"], check=True, stdout=DEVNULL)
        except CalledProcessError as e:
            raise FileTransferError(
                "Make sure you have an entry for consucom in ~/.ssh/config."
            ) from e

    def test_connections(self):
        self.test_mongo()
        self.test_ssh()

    def transfer(self) -> "FileTransfer":
        self.filetransfer_file_upload()
        self.filetransfer_database_entry()
        return self

    def notify(self,
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
