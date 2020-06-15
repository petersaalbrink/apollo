"""Module that manages the environment variables needed by common.

This module provides a function, `getenv`, which when imported first
loads the necessary environment variables, and then can be used to
return those. This module is used by common internally, but it can be
used externally, if one needs to load secrets or hostnames into the
system's environment variables outside of common.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

commondir = Path.home() / ".common"
envfile = Path(commondir / ".env")


def getenv():
    """Load the .env file that is used by common."""

    # Create .env, if it doesn't exist yet
    if not envfile.exists():
        try:
            envfile.parent.mkdir()
        except FileExistsError:
            pass
        env = Path(__file__).parent / "etc/.env"
        with open(env) as src, open(envfile, "w") as dst:
            dst.write(src.read())

    # Load .env
    load_dotenv(dotenv_path=envfile, override=False)


def _write_pem():
    """Create certificates needed by common.MySQLClient.

    The certificates need to be present as environment variables and
    will be written to the current working directory.
    """
    keys = (
        "CLIENT_CERT",
        "CLIENT_KEY",
        "SERVER_CA",
    )
    for key in keys:
        data = os.environ[f"MX_MYSQL_{key}"]
        for s in ("-----BEGIN CERTIFICATE-----", "-----BEGIN RSA PRIVATE KEY-----"):
            data = data.replace(s, f"{s}\n")
        for s in ("-----END CERTIFICATE-----", "-----END RSA PRIVATE KEY-----"):
            data = data.replace(s, f"\n{s}\n")
        pem = key.replace("_", "-").lower()
        with open(Path(Path.cwd() / f"{pem}.pem"), "w") as f:
            f.write(data)


getenv()
getenv = os.getenv
