from pathlib import Path
from typing import Tuple
from base64 import b64encode

FILE = Path(Path.home() / ".common/.secrets")
NAMES = {
    "es": "Elasticsearch dev server",
    "mongo": "MongoDB dev server",
    "addr": "MongoDB addressvalidation server",
    "sql": "MySQL dev server",
    "ccv": "CCV FTP server",
    "bk": "BuurtKadoos FTP server",
    "ng": "NutsGroep FTP server",
    "ftp": "VPS11 FTP server",
}


def create_secrets():
    if not FILE.exists():
        try:
            FILE.parent.mkdir()
        except FileExistsError:
            pass
        with open(FILE, "w") as f:
            f.write("mail_pass::::TmtUZ01wbThvVDNjSzk1NA==\n")


def ask_secret(name: str) -> Tuple[str, bytes]:
    create_secrets()
    usr = input(f"{NAMES[name]} username: ")
    pwd = input(f"{NAMES[name]} password: ")
    pwd = b64encode(bytes(pwd.encode())).decode()
    with open(FILE, "a") as f:
        f.write(f"{name}::{usr}::{pwd}\n")
    return usr, bytes(pwd.encode())


def get_secret(name: str) -> Tuple[str, bytes]:
    with open(FILE) as f:
        for line in f:
            key, usr, pwd = line.rstrip("\r\n").split("::")
            if key == name:
                return usr, bytes(pwd.encode())
    return ask_secret(name)
