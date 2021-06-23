"""Send emails using Matrixian dev account."""

from __future__ import annotations

__all__ = (
    "DEFAULT_EMAIL",
    "EmailClient",
)

from collections.abc import Sequence
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
from pathlib import Path
from smtplib import SMTP
from sys import argv
from traceback import format_exc
from zipfile import ZIP_LZMA, ZipFile

from ..secrets import getenv

DEFAULT_EMAIL = getenv("MX_DEFAULT_EMAIL") or "datateam@matrixiangroup.com"


class EmailClient:
    """Client for sending plain text emails and attachments.

    The main method for sending emails is EmailClient.send_email().
    """

    def __init__(
        self,
        smtp_server: str = "smtp.gmail.com:587",
        login: str | None = None,
        password: str | None = None,
    ):
        """Client for sending plain text emails and attachments.

        If you don't provide a login or a password, these are read from
        environment variables MX_MAIL_USR and MX_MAIL_PWD.
        """
        self.__server: SMTP | None = None
        self._smtp_server = smtp_server
        if not login or not password:
            from ..secrets import get_secret

            self._login, self._password = get_secret("MX_MAIL")
        else:
            self._login, self._password = login, password

    def _connect(self) -> None:
        """Connect to the SMTP server."""
        self._server.starttls()
        self._server.login(self._login, self._password)

    @property
    def _server(self) -> SMTP:
        if not self.__server:
            self.__server = SMTP(self._smtp_server)
        return self.__server

    def connection(self) -> bool:
        """Test the connection to the SMTP server."""
        self._connect()
        self._server.quit()
        return True

    def send_email(
        self,
        to_address: str | Sequence[str] = DEFAULT_EMAIL,
        subject: str | None = None,
        message: str | Exception | None = None,
        from_address: str | None = None,
        attachment_path: str | Path | Sequence[str | Path] | None = None,
        error_message: bool = False,
    ) -> None:
        """Send an email to an email address (str) or a list of addresses.

        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory.

        To send a Python traceback (in an except-clause), set error_message to True.
        """

        if not from_address:
            from_address = self._login
        assert isinstance(from_address, str)

        msg = MIMEMultipart()
        msg["From"] = from_address
        if isinstance(to_address, str):
            msg["To"] = to_address
        elif isinstance(to_address, Sequence):
            msg["To"] = ",".join(to_address)
        if not subject:
            if argv[0] not in {"", "-c"}:
                subject = Path(argv[0]).stem
            else:
                subject = Path.cwd().stem
        msg["Subject"] = subject
        if not message:
            message = ""
        elif isinstance(message, Exception):
            message = f"{message}"
        if error_message:
            message = f"{message}\n\n{format_exc()}" if message else format_exc()
        mime = MIMEText(
            message,
            "html" if message.lower().startswith("<!doctype html>") else "plain",
        )
        msg.attach(mime)

        if attachment_path:
            if not isinstance(attachment_path, Sequence):
                filename = f"{Path(attachment_path).resolve().stem}.zip"
                attachment_path = [attachment_path]
            elif subject:
                filename = f"{subject}.zip"
            else:
                filename = "attachment.zip"

            with BytesIO() as zipped:
                with ZipFile(zipped, "a", compression=ZIP_LZMA, allowZip64=False) as zf:
                    for attachment in attachment_path:
                        with open(attachment, encoding="latin-1") as f:
                            zf.writestr(Path(attachment).name, f.read())
                    for fn in zf.filelist:
                        fn.create_system = 0
                payload = zipped.getvalue()

            p = MIMEBase("application", "octet-stream")
            p.set_payload(payload)
            encode_base64(p)
            p.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(p)

        self._connect()
        self._server.sendmail(from_address, to_address, msg.as_string())
        self._server.quit()
