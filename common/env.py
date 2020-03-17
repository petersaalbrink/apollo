import os
from pathlib import Path
from dotenv import load_dotenv

commondir = Path.home() / ".common"
envfile = Path(commondir / ".env")


def getenv():

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
    keys = (
        "CLIENT_CERT",
        "CLIENT_KEY",
        "SERVER_CA",
    )
    for key in keys:
        data = os.environ[f"MX_MYSQL_{key}"]
        pem = key.replace("_", "-").lower()
        with open(Path(Path.cwd() / f"{pem}.pem"), "w") as f:
            f.write(data)


getenv()
getenv = os.getenv
