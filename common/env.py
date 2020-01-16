import os
from pathlib import Path
from dotenv import load_dotenv


def getenv():
    file = Path(Path.home() / ".common/.env")

    # Create .env, if it doesn't exist yet
    if not file.exists():
        try:
            file.parent.mkdir()
        except FileExistsError:
            pass
        env = Path(__file__).parents[1] / ".env"
        with open(env) as src, open(file, "w") as dst:
            dst.write(src.read())

    # Load .env
    load_dotenv(dotenv_path=file, override=True)


getenv()
getenv = os.getenv
