class Credentials:
    __slots__ = ["usr", "pwd"]

    def __init__(self, usr: str, pwd: str):
        self.usr = usr
        self.pwd = pwd


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
