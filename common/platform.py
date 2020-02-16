from datetime import datetime
from pathlib import Path
from secrets import token_hex
from subprocess import run, CalledProcessError
from time import time
from typing import List, Union
from bson import DBRef, ObjectId
from pendulum import timezone
from pymongo.errors import PyMongoError
from .connectors.email import EmailClient
from .connectors.mongodb import MongoDB


class FileTransfer:
    def __init__(self,
                 user_id: str = None,
                 username: str = None,
                 filename: str = None):

        db = MongoDB("production_api.user", host="prod")
        self.db = MongoDB("production_api.filetransferFile", host="prod")
        self.test_connections()

        if (not user_id and not username) or (user_id and username):
            raise ValueError("Provide either a name or ID for the user.")
        elif user_id:
            self.user_id = user_id
            self.username = db.find_one({"_id": ObjectId(user_id)})["username"]
        elif username:
            self.user_id = db.find_one({"username": username})["_id"]
            self.username = username

        self.filename = filename
        self.insert_filename = Path(filename).name
        self.filepath = (
            f"/var/www/platform_projects/public/upload/filetransfer"
            f"/{user_id}/{token_hex(10)}{round(time())}")
        self.cmds = (
            f"""ssh consucom "install -d -m 0777 {self.filepath}" """,
            f"""scp {self.filename} consucom:{self.filepath}/ """,
            f"""ssh consucom "chmod 777 {self.filepath}/{self.insert_filename}" """
        )
        self.doc = {
            "createdDate": datetime.now(tz=timezone("Europe/Amsterdam")),
            "filePath": self.filepath,
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
            run(cmd, shell=True).check_returncode()
        return self

    def test_mongo(self):
        try:
            self.db.find_one()
        except PyMongoError as e:
            raise RuntimeError(
                "Make sure you have access to MongoDB prod/live server."
            ) from e

    @staticmethod
    def test_ssh():
        try:
            run("ssh consucom echo ssh ok", shell=True).check_returncode()
        except CalledProcessError as e:
            raise RuntimeError(
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
               to_address: Union[str, List[str]],
               username: str = None
               ) -> "FileTransfer":
        template = Path(__file__).parent / "etc/email.html"
        with open(template) as f:
            message = f.read()
        message = message.replace("USERNAME", username or self.username)
        message = message.replace("FILENAME", self.insert_filename)
        EmailClient().send_email(
            to_address=to_address,
            subject="Nieuw bestand in Filetransfer",
            message=message,
        )
        return self
