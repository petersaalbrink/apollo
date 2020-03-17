import os
from pathlib import Path
from dotenv import load_dotenv

envfile = Path(Path.home() / ".common/.env")


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
    load_dotenv(dotenv_path=file, override=False)


getenv()
getenv = os.getenv
