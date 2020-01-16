from collections import namedtuple
from pathlib import Path
from getpass import getpass
from base64 import b64encode, b64decode
try:
    import termios
    _ = termios.tcgetattr, termios.tcsetattr
except (ImportError, AttributeError):
    getpass = input

NAMES = {
    "es": "Elasticsearch dev server",
    "mongo": "MongoDB dev server",
    "addr": "MongoDB addressvalidation server",
    "sql": "MySQL dev server",
    "ccv": "CCV FTP server",
    "bk": "BuurtKadoos FTP server",
    "ng": "NutsGroep FTP server",
    "ftp": "VPS11 FTP server",
    "mongo_stg": "MongoDB stg server",
    "mongo_prod": "MongoDB prod server",
    "bstorage": "Matrixian Synaman File Transfer",
    "da": "DigitalAudience FTP server",
    "platform": "Matrixian Platform",
    "dev_platform": "Matrixian Platform (development)",
}

Credentials = namedtuple("Credentials", ("usr", "pwd"))


def change_secret(name: str) -> Credentials:

    file = Path(Path.home() / ".common/.secrets")

    # Remove the line from the file and re-write it
    with open(file) as f:
        data = [line for line in f if not line.startswith(name)]
    with open(file, "w") as f:
        f.writelines(data)

    # Add new secret
    return get_secret(name=name)


def get_secret(name: str) -> Credentials:

    file = Path(Path.home() / ".common/.secrets")

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
            secret = line.rstrip("\r\n").split("::")
            if secret != [""]:
                key, usr, pwd = secret
                if key == name:
                    return Credentials(usr, b64decode(bytes(pwd.encode())).decode())

    # Ask secret, if needed
    usr = input(f"{NAMES.get(name, name)} username: ")
    pwd = getpass(f"{NAMES.get(name, name)} password: ")
    pwd = b64encode(bytes(pwd.encode())).decode()
    if pwd:
        with open(file, "a") as f:
            f.write(f"{name}::{usr}::{pwd}\n")

    return Credentials(usr, b64decode(bytes(pwd.encode())).decode())
