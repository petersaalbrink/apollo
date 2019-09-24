from contextlib import AbstractContextManager


class Credentials(AbstractContextManager):
    __slots__ = ["usr", "pwd"]

    def __init__(self, usr: str, pwd: str):
        self.usr = usr
        self.pwd = pwd

    def __str__(self):
        return f"{self.__class__.__name__}(usr='{self.usr}', pwd=<hidden>)"

    def __repr__(self):
        return f"{self.__class__.__name__}(usr='{self.usr}', pwd=<hidden>)"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


del AbstractContextManager


def get_secret(name: str) -> Credentials:
    # Funtion imports
    from pathlib import Path
    from getpass import getpass, GetPassWarning
    from warnings import simplefilter
    from base64 import b64encode, b64decode

    # Set defaults
    file = Path(Path.home() / ".common/.secrets")
    names = {
        "es": "Elasticsearch dev server",
        "mongo": "MongoDB dev server",
        "addr": "MongoDB addressvalidation server",
        "sql": "MySQL dev server",
        "ccv": "CCV FTP server",
        "bk": "BuurtKadoos FTP server",
        "ng": "NutsGroep FTP server",
        "ftp": "VPS11 FTP server",
        "mongo_stg": "MongoDB stg server",
        "bstorage": "Matrixian Synaman File Transfer",
        "da": "DigitalAudience FTP server",
        "platform": "Matrixian Platform",
    }

    # Create secrets, if it doesn't exist yet
    if not file.exists():
        try:
            file.parent.mkdir()
        except FileExistsError:
            pass
        with open(file, "w") as f:
            f.write("mail_pass::::TmtUZ01wbThvVDNjSzk1NA==\n")

    # Get secret, if it has been saved
    with open(file) as f:
        for line in f:
            key, usr, pwd = line.rstrip("\r\n").split("::")
            if key == name:
                return Credentials(usr, b64decode(bytes(pwd.encode())).decode())

    # Ask secret, if needed
    simplefilter("error")
    usr = input(f"{names.get(name, name)} username: ")
    try:
        pwd = getpass(f"{names.get(name, name)} password: ")
    except GetPassWarning:
        pwd = input(f"{names.get(name, name)} password: ")
    pwd = b64encode(bytes(pwd.encode())).decode()
    with open(file, "a") as f:
        f.write(f"{name}::{usr}::{pwd}\n")
    return Credentials(usr, b64decode(bytes(pwd.encode())).decode())
