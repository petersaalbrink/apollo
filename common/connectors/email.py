from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
from pathlib import Path
from smtplib import SMTP
from sys import argv
from traceback import format_exc
from typing import List, Union
from zipfile import ZipFile, ZIP_LZMA


class EmailClient:
    """Client for sending plain text emails and attachments."""

    def __init__(self,
                 smtp_server: str = "smtp.gmail.com:587",
                 login: str = None,
                 password: str = None):
        """Client for sending plain text emails and attachments."""
        self._server = None
        self._smtp_server = smtp_server
        if not login or not password:
            from common.secrets import get_secret
            self._login, self._password = get_secret("MX_MAIL")
        else:
            self._login, self._password = login, password

    def _connect(self):
        self._server = SMTP(self._smtp_server)
        self._server.starttls()
        self._server.login(self._login, self._password)

    def connection(self):
        self._connect()
        self._server.quit()
        return True

    def send_email(self,
                   to_address: Union[str, list] = "psaalbrink@matrixiangroup.com",
                   subject: str = None,
                   message: Union[str, Exception] = None,
                   from_address: str = None,
                   attachment_path: Union[Union[str, Path], List[Union[str, Path]]] = None,
                   error_message: bool = False,
                   ):
        """Send an email to an email address (str) or a list of addresses.
        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory."""

        if not from_address:
            from_address = self._login

        msg = MIMEMultipart()
        msg["From"] = from_address
        if isinstance(to_address, str):
            msg["To"] = to_address
        elif isinstance(to_address, list):
            msg["To"] = ",".join(to_address)
        if not subject:
            subject = Path(argv[0]).stem
        msg["Subject"] = subject
        if not message:
            message = ""
        elif isinstance(message, Exception):
            message = f"{message}"
        if error_message:
            message = (f"{message}\n\n{format_exc()}"
                       if message else format_exc())
        message = MIMEText(
            message,
            "html" if message.lower().startswith("<!doctype html>") else "plain")
        msg.attach(message)

        if attachment_path:
            if not isinstance(attachment_path, list):
                filename = f"{Path(attachment_path).stem}.zip"
                attachment_path = [attachment_path]
            elif subject:
                filename = f"{subject}.zip"
            else:
                filename = "attachment.zip"

            zipped = BytesIO()
            zf = ZipFile(zipped, "a", compression=ZIP_LZMA, allowZip64=False)

            for attachment in attachment_path:
                with open(attachment, "r") as f:
                    zf.writestr(Path(attachment).name, f.read())

            for f in zf.filelist:
                f.create_system = 0
            zipped.seek(0)
            zipped = zipped.read()
            zf.close()

            p = MIMEBase("application", "octet-stream")
            p.set_payload(zipped)
            encode_base64(p)
            p.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(p)

        self._connect()
        self._server.sendmail(from_address, to_address, msg.as_string())
        self._server.quit()
