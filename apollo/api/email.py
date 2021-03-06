from __future__ import annotations

__all__ = (
    "ERROR_CODES",
    "check_email",
    "validate_email",
)

import re
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from copy import copy
from datetime import datetime as dt
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from smtplib import SMTP, SMTPServerDisconnected
from threading import Thread
from typing import Any

import dns.resolver as resolve
from babel import Locale, UnknownLocaleError
from dns.exception import DNSException
from pymailcheck import suggest
from requests.exceptions import RequestException
from urllib3.exceptions import ReadTimeoutError

from ..connectors.mx_mongo import MongoDB, MxDatabase
from ..requests import get

PATH = Path(__file__).parents[1] / "etc"
LIVE = "136.144.203.100"
URL = f"http://{LIVE}:4000/email?email="  # noqa

ERROR_CODES = {
    421: "Service not available, closing transmission channel",
    432: "A password transition is needed",
    450: "Requested mail action not taken: mailbox unavailable",
    451: "IMAP server unavailable",
    452: "Requested action not taken: insufficient system storage",
    454: "Temporary authentication failure",
    455: "Server unable to accommodate parameters",
    500: "Authentication Exchange line is too long",
    501: "Client initiated Authentication Exchange",
    502: "Command not implemented",
    503: "Bad sequence of commands",
    504: "Unrecognized authentication type",
    521: "Server does not accept mail",
    523: "Encryption Needed",
    530: "Authentication required",
    534: "Authentication mechanism is too weak",
    535: "Authentication credentials invalid",
    538: "Encryption required for requested authentication mechanism",
    550: "Requested action not taken: mailbox unavailable",
    551: "User not local; please try <forward-path>",
    552: "Requested mail action aborted: exceeded storage allocation",
    553: "Requested action not taken: mailbox name not allowed",
    554: "Message too big for system",
    556: "Domain does not accept mail",
}

with open(PATH / "disposable_providers.txt") as f:
    _disposable_providers = [x.rstrip().lower() for x in f]
with open(PATH / "free_providers.txt") as f:
    _free_providers = [x.rstrip().lower() for x in f]
_db = MongoDB("cdqc")


class _EmailValidator:
    english_tlds = {"com", "icu", "info", "net", "org", "tk", "uk", "xyz"}
    tlds = (*english_tlds, "nl", "be", "cn", "de", "ga", "ru")
    email_regex = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
    syntax_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,6}")
    at_words_regex = re.compile(r"[a-zA-Z]*@[a-zA-Z]*")
    disposable_providers = _disposable_providers
    free_providers = _free_providers
    assert isinstance(_db, MxDatabase)
    mongo_cache = _db["email_checker_cache"]
    mongo_mx = _db["email_checker_mx_records"]
    td = timedelta(days=90)

    def __init__(
        self,
        email: str,
        check_accept_all: bool,
        use_cache: bool,
        debug: bool,
    ):
        self.CHECK_ACCEPT_ALL = check_accept_all
        self.USE_CACHE = use_cache
        self.DEBUG = debug
        self.ORIGINAL_EMAIL = copy(email)
        self.EMAIL = copy(email)

        self.START = dt.now()

        self.OUTPUT_DICT: dict[str, Any] = {
            "accept_all": False,
            "corrected": False,
            "disposable": False,
            "domain": None,
            "email": self.EMAIL,
            "free": False,
            "language": "English",
            "mx_code": None,
            "mx_record": None,
            "name": {
                "first": None,
                "gender": None,
                "last": None,
            },
            "qualification": None,
            "safe_to_send": True,
            "status": None,
            "success": True,
            "time": None,
            "user": None,
        }
        self._futures: list[Future[None]] = []

    def parse_and_correct(self) -> None:

        emails = self.email_regex.findall(self.EMAIL)
        if emails:
            if self.EMAIL != emails[0]:
                self.OUTPUT_DICT["corrected"] = True
            self.EMAIL = emails[0]
            return

        corrected_domain = suggest(self.EMAIL)
        if corrected_domain and self.EMAIL != corrected_domain["full"]:
            self.EMAIL = corrected_domain["full"]
            self.OUTPUT_DICT["corrected"] = True
            return

        all_at_words = self.at_words_regex.findall(self.EMAIL)
        if all_at_words:
            self.EMAIL = all_at_words[0]
            for tld in self.tlds:
                if self.EMAIL.endswith(tld):
                    self.EMAIL = f"{self.EMAIL[:-len(tld)]}.{tld}"
                    self.OUTPUT_DICT["corrected"] = True

    def check_syntax(self) -> None:
        matched = self.syntax_regex.match(self.EMAIL)
        if not matched or len(self.EMAIL.strip()) > 320:
            raise ValueError

    def _connect(self, rcpt: str) -> tuple[int, str]:

        server = SMTP(timeout=10)

        if self.DEBUG:
            server.set_debuglevel(1)
        else:
            server.set_debuglevel(0)

        mx_record = self.OUTPUT_DICT["mx_record"]
        assert isinstance(mx_record, str)
        server.connect(mx_record)

        server.helo()
        server.mail("my@from.addr.ess")
        code, message = server.rcpt(rcpt)
        server.quit()

        return code, message.decode().lower()

    def check_accept_all(self) -> None:
        if not self.CHECK_ACCEPT_ALL:
            return

        code, _ = self._connect(f"70206294287020629428@{self.OUTPUT_DICT['domain']}")

        if code == 250:
            self.OUTPUT_DICT["accept_all"] = True
            self.OUTPUT_DICT["safe_to_send"] = False

    def check_user(self) -> None:

        code, message = self._connect(self.EMAIL)

        self.OUTPUT_DICT["mx_code"] = code

        if code == 250:
            self.OUTPUT_DICT["status"] = "OK"
            self.OUTPUT_DICT["qualification"] = "OK"
        elif (
            "4.1.8" in message
            or "5.1.8" in message
            or "5.7.1" in message
            or "authentication required" in message
            or "block" in message
            or "list" in message
            or "not yet authorized" in message
            or "relay access denied" in message
            or "relay not permitted" in message
            or "relaying denied from" in message
            or "sender address rejected" in message
            or "sender verify failed" in message
        ):
            self.OUTPUT_DICT["status"] = "WARNING"
            self.OUTPUT_DICT["qualification"] = f"Not Permitted ({code})"
        else:
            self.OUTPUT_DICT["safe_to_send"] = False
            self.OUTPUT_DICT["status"] = "USELESS"
            self.OUTPUT_DICT["qualification"] = f"Not Permitted ({code})"

    def _parse_user_name(self, user: str) -> None:
        from ..persons import parse_name  # importing here to avoid circular import

        parsed = parse_name(user)
        self.OUTPUT_DICT["name"].update(
            {
                "first": parsed.first,
                "gender": parsed.gender,
                "last": parsed.last,
            }
        )

    def parse_user_domain_country(self, executor: ThreadPoolExecutor) -> None:
        split_address = self.EMAIL.split("@")

        # Parse user
        self.OUTPUT_DICT["user"] = split_address[0]
        self._futures.append(executor.submit(self._parse_user_name, split_address[0]))

        # Parse domain
        self.OUTPUT_DICT["domain"] = str(split_address[1])

        # Parse country
        tld = split_address[1].split(".")[-1]
        if tld not in self.english_tlds:
            try:
                self.OUTPUT_DICT["language"] = Locale.parse(f"und_{tld}").language_name
            except (ValueError, UnknownLocaleError):
                pass

    def check_disposable(self) -> None:
        if self.OUTPUT_DICT["domain"].lower() in self.disposable_providers:
            self.OUTPUT_DICT["disposable"] = True
            self.OUTPUT_DICT["safe_to_send"] = False

    def check_free(self) -> None:
        if self.OUTPUT_DICT["domain"].lower() in self.free_providers:
            self.OUTPUT_DICT["free"] = True

    def time_spent(self, ret: bool = False) -> str | None:
        if ret:
            return str(int((dt.now() - self.START).total_seconds() * 1000))
        else:
            self.OUTPUT_DICT["time"] = str(
                int((dt.now() - self.START).total_seconds() * 1000)
            )
            return None

    def search_cache(self) -> dict[str, Any] | None:
        if self.USE_CACHE:
            result = self.mongo_cache.find_one(
                {
                    "email": self.EMAIL,
                    "created_at": {"$gte": dt.now() - self.td},
                }
            )

            if result and "output" in result and result["output"]:
                result["output"]["time"] = self.time_spent(True)
                return result["output"]
        return None

    def save_request(self) -> None:
        if self.USE_CACHE:
            self.mongo_cache.insert_one(
                {
                    "email": self.ORIGINAL_EMAIL,
                    "check_accept_all": self.CHECK_ACCEPT_ALL,
                    "output": self.OUTPUT_DICT,
                    "created_at": dt.now(),
                }
            )

    def get_mx_records(self) -> None:

        cached_mx_records = self.mongo_mx.find_one(
            {"domain": self.OUTPUT_DICT["domain"]}
        )

        if cached_mx_records:
            self.OUTPUT_DICT["mx_record"] = cached_mx_records["mx_records"][0]
        else:
            records = resolve.resolve(self.OUTPUT_DICT["domain"], "MX")
            mx_records = [record.exchange.to_text() for record in records]

            if mx_records:
                self.OUTPUT_DICT["mx_record"] = mx_records[0]

            self.mongo_mx.insert_one(
                {
                    "domain": self.OUTPUT_DICT["domain"],
                    "mx_records": mx_records,
                    "accept_all": False,
                }
            )

    def validate_email(self) -> dict[str, Any]:
        with ThreadPoolExecutor() as executor:
            try:

                # Searches the cache
                cache = self.search_cache()
                if cache:
                    return cache

                # Parse input and correct email
                self.parse_and_correct()

                # Check email syntax
                self.check_syntax()

                # Parse domain and country from email
                self.parse_user_domain_country(executor)

                # Get MX records - Searches in mx_records collection if previously queried
                future = executor.submit(self.get_mx_records)

                # Update output dictionary
                self.OUTPUT_DICT["email"] = self.EMAIL

                # Check if domain is from a disposable mail provider
                self.check_disposable()

                # Check if domain is from a free mail provider
                self.check_free()

                # wait for MX record
                future.result()

                # Check if user exists on mailserver
                self._futures.append(executor.submit(self.check_user))

                # Check if mailserver accepts all user names
                self._futures.append(executor.submit(self.check_accept_all))

                # wait for: check_user check_accept_all parse_name
                for future in self._futures:
                    future.result()

                # Save the request into cache collection on Mongo
                Thread(target=self.save_request).start()

                # Update output dictionary with the request time
                self.time_spent()

                return self.OUTPUT_DICT

            except ValueError:
                if self.DEBUG:
                    traceback.print_exc()
                self.OUTPUT_DICT.update(
                    {
                        "email": self.EMAIL,
                        "qualification": "Invalid e-mail address format",
                        "safe_to_send": False,
                        "status": "MANREV",
                        "time": self.time_spent(True),
                    }
                )
                return self.OUTPUT_DICT

            except (DNSException, OSError, SMTPServerDisconnected):
                if self.DEBUG:
                    traceback.print_exc()
                self.OUTPUT_DICT.update(
                    {
                        "email": self.EMAIL,
                        "qualification": "SMTP Invalid",
                        "safe_to_send": False,
                        "status": "USELESS",
                        "time": self.time_spent(True),
                    }
                )
                return self.OUTPUT_DICT

            except Exception:  # noqa
                if self.DEBUG:
                    traceback.print_exc()
                self.OUTPUT_DICT.update(
                    {
                        "email": self.EMAIL,
                        "qualification": "Exception",
                        "safe_to_send": False,
                        "status": "EXCEPTION",
                        "success": False,
                        "time": self.time_spent(True),
                    }
                )
                return self.OUTPUT_DICT


def validate_email(
    email: str,
    check_accept_all: bool,
    use_cache: bool,
    debug: bool = False,
    try_again: bool = False,
) -> dict[str, Any]:
    response = _EmailValidator(
        email=email,
        check_accept_all=check_accept_all,
        use_cache=use_cache,
        debug=debug,
    ).validate_email()
    if try_again and response["mx_code"] != 250:
        response = _EmailValidator(
            email=email,
            check_accept_all=check_accept_all,
            use_cache=False,
            debug=debug,
        ).validate_email()
    return response


@lru_cache
def check_email(
    email: str,
    safe_to_send: bool = False,
) -> dict[str, Any] | bool:
    """Use Matrixian's Email Checker API to validate an email address.

    Optionally, set safe_to_send to True for boolean output.
    """
    try:
        response = get(
            f"{URL}{email}",
            text_only=True,
            timeout=30,
        )
    except (ConnectionError, ReadTimeoutError, RequestException):
        response = {
            "email": email,
            "safe_to_send": False,
        }
    assert isinstance(response, dict)
    if safe_to_send:
        return response["safe_to_send"]
    return response
