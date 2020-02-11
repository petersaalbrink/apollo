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
    def __init__(self, user_id: str, filename: str):
        self.user_id = user_id
        self.filename = filename
        self.filepath = (
            f"/var/www/platform_projects/public/upload/filetransfer"
            f"/{user_id}/{token_hex(10)}{round(time())}")
        self.db = MongoDB("production_api.filetransferFile", host="prod")

    def filetransfer_database_entry(self) -> "FileTransfer":
        self.db.insert_one({
            "createdDate": datetime.now(tz=timezone("Europe/Amsterdam")),
            "filePath": self.filepath,
            "fileName": self.filename,
            "fileSize": Path(self.filename).stat().st_size,
            "downloadsCount": 0,
            "creator": DBRef("user", ObjectId("5d8b7cee7704cf416b41ce93"), "production_api"),
            "owner": DBRef("user", ObjectId(self.user_id), "production_api")
        })
        return self

    def filetransfer_file_upload(self) -> "FileTransfer":
        for cmd in (
                f"""ssh consucom "install -d -m 0777 {self.filepath}" """,
                f"""scp {self.filename} consucom:{self.filepath}/ """
                f"""ssh consucom "chmod 777 {self.filepath}/{self.filename}" """
        ):
            run(cmd, shell=True)
        return self

    def test_mongo(self):
        try:
            self.db.find_one()
        except PyMongoError:
            raise RuntimeError(
                "Make sure you have access to MongoDB prod/live server.")

    @staticmethod
    def test_ssh():
        try:
            run("ssh consucom echo ok", shell=True).check_returncode()
        except CalledProcessError:
            raise RuntimeError(
                "Make sure you have an entry for consucom in ~/.ssh/config.")

    def test_connections(self):
        self.test_mongo()
        self.test_ssh()

    def transfer(self) -> "FileTransfer":
        self.test_connections()
        self.filetransfer_file_upload()
        self.filetransfer_database_entry()
        return self

    def notify(self,
               to_address: Union[str, List[str]],
               username: str
               ) -> "FileTransfer":
        template = Path(__file__).parent / "etc/email.html"
        with open(template) as f:
            message = f.read()
        message = message.replace("USERNAME", username)
        message = message.replace("FILENAME", self.filename)
        EmailClient().send_email(
            to_address=to_address,
            subject="Nieuw bestand in Filetransfer",
            message=message,
        )
        return self
