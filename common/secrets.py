"""Module that provides access to retrieving and storing secrets.

The main method for accessing secrets is the `get_secret` function.
Secrets are retrieved from the .env file in the ~/.common directory and
decoded. If a secret is not present or is missing, the user will be
asked to provide it (using stdin). The provided secret will then be
encoded and stored for later use. A secret, consisting of a username
and a password, will be returned as `Credentials`, a namedtuple with
attributes `usr` and `pwd`.

This module also contains a `get_token` function, which returns headers
containing an access token for the Matrixian Platform.
"""

from base64 import b64encode, b64decode
from collections import namedtuple
from contextlib import suppress
from json import loads
from pathlib import Path
from re import compile
from requests import post
try:
    from getpass import getpass
    import termios  # noqa
    _ = termios.tcgetattr, termios.tcsetattr
except (ImportError, AttributeError):
    getpass = input
from .env import getenv

Credentials = namedtuple("Credentials", ("usr", "pwd"))


def change_secret(name: str) -> Credentials:
    """Change an existing secret in the ~/.common/.env file.

    You will be asked to provide the secret using stdin. The secret
    will be encoding before storing it for later use, and the secret
    will be returned as `Credentials`, a namedtuple with attributes
    `usr` and `pwd`.
    """
    names = {
        "MX_ELASTIC": "Elasticsearch servers",
        "MX_FTP_BK": "BuurtKadoos FTP server",
        "MX_FTP_BSTORAGE": "Matrixian Synaman File Transfer",
        "MX_FTP_CCV": "CCV FTP server",
        "MX_FTP_DA": "DigitalAudience FTP server",
        "MX_FTP_NG": "NutsGroep FTP server",
        "MX_FTP_VPS": "VPS11 FTP server",
        "MX_MAIL": "EmailClient account",
        "MX_MONGO_ADDR": "MongoDB addressvalidation server",
        "MX_MONGO_CDQC": "MongoDB CDQC server",
        "MX_MONGO_DEV": "MongoDB dev server",
        "MX_MONGO_PROD": "MongoDB prod server",
        "MX_MYSQL_DEV": "MySQL dev server",
        "MX_MYSQL_PR": "MySQL Postregister server",
        "MX_PLATFORM_DATA": "Matrixian Platform (Data Team)",
        "MX_PLATFORM_DEV": "Matrixian Platform (development)",
        "MX_PLATFORM_PROD": "Matrixian Platform",
        "MX_WEBHOOK_PETER": "Flask Webhook (peter)",
        "MX_WEBHOOK_DATATEAM": "Flask Webhook (datateam)",
    }
    usr = input(f"{names.get(name, name)} username: ")
    pwd = getpass(f"{names.get(name, name)} password: ")
    pwd = b64encode(bytes(pwd.encode())).decode()
    if pwd:
        re = compile(r"=.*\n")
        file = Path(Path.home() / ".common/.env")
        with open(file) as f:
            curr_data = [line for line in f]
        new_data = [re.sub(f"={usr}\n", line)
                    if line.startswith(f"{name}_USR")
                    else line for line in curr_data]
        new_data = [re.sub(f"={pwd}\n", line)
                    if line.startswith(f"{name}_PWD")
                    else line for line in new_data]
        if curr_data == new_data:
            new_data.extend([f"{name}_USR={usr}\n",
                             f"{name}_PWD={pwd}\n"])
        with open(file, "w") as f:
            f.writelines(new_data)

    # Decode secret and return
    pwd = b64decode(bytes(pwd.encode())).decode()
    secret = Credentials(usr, pwd)

    return secret


def get_secret(name: str) -> Credentials:
    """Get an existing secret from the system's environment variables.

    Environment variables will be first be loaded using the
    ~/.common/.env file.

    If a secret does not yet exist, you will be asked to provide the
    secret (using stdin). The secret will be encoding before storing it
    for later use.

    The secret will be returned as `Credentials`, a namedtuple with
    attributes `usr` and `pwd`.
    """
    from .env import getenv

    # Read secret from environment variables
    usr = getenv(f"{name}_USR")
    pwd = getenv(f"{name}_PWD")
    if pwd:
        pwd = b64decode(bytes(pwd.encode())).decode()

    # Ask secret, if needed
    if not usr or not pwd:
        usr, pwd = change_secret(name)

    # Return secret
    secret = Credentials(usr, pwd)
    return secret


def get_token() -> dict:
    """Return headers with an access token for the Matrixian Platform."""
    usr, pwd = get_secret("MX_PLATFORM_DATA")
    while True:
        with suppress(KeyError):
            posted = post("https://api.matrixiangroup.com/token",
                          data={"username": usr,
                                "password": pwd})
            token = loads(posted.text)["access_token"]
            break
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
