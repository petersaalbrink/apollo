from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path, PurePath
from smtplib import SMTP
from sys import argv
from typing import Union


class EmailClient:
    """Client for sending plain text emails and attachments."""
    from common.secrets import get_secret
    mail = get_secret("mail_pass")

    def __init__(self,
                 smtp_server="smtp.gmail.com:587",
                 login="dev@matrixiangroup.com",
                 password=mail.pwd):
        """Client for sending plain text emails and attachments."""
        self._smtp_server = smtp_server
        self._login = login
        self._password = password

    def send_email(self,
                   to_address: Union[str, list] = "psaalbrink@matrixiangroup.com",
                   subject: str = None,
                   message: Union[str, Exception] = None,
                   from_address: str = "dev@matrixiangroup.com",
                   attachment_path: Union[str, PurePath] = None,
                   error_message: bool = False,
                   ):
        """Send an email to an email address (str) or a list of addresses.
        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory."""

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
            message = str(message)
        if error_message:
            from sys import exc_info
            from traceback import format_exception
            message = f"{message}\n\n{''.join(format_exception(*exc_info()))}" \
                if message else "".join(format_exception(*exc_info()))
        msg.attach(MIMEText(message, "plain"))

        if attachment_path:
            p = MIMEBase("application", "octet-stream")
            with open(attachment_path, "rb") as attachment:
                p.set_payload(attachment.read())
            encode_base64(p)
            filename = attachment_path.name if isinstance(attachment_path, PurePath) \
                else attachment_path.split("/")[-1]
            p.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(p)

        server = SMTP(self._smtp_server)
        server.starttls()
        server.login(self._login, self._password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()
