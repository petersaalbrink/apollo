__all__ = (
    "check_email",
    "validate_email",
)

from copy import copy
from datetime import datetime as dt, timedelta
from functools import lru_cache
from pathlib import Path
import re
from smtplib import SMTP, SMTPServerDisconnected
import traceback
from typing import Optional, Union

from babel import Locale, UnknownLocaleError
import dns.resolver as resolve
from dns.exception import DNSException
from pymailcheck import suggest

from ..connectors.mx_mongo import MongoDB
from ..requests import get

PATH = Path(__file__).parents[1] / "etc"
LIVE = "136.144.203.100"
URL = f"http://{LIVE}:4000/email?email="


class _EmailValidator:
    english_tlds = {"com", "icu", "info", "net", "org", "tk", "uk", "xyz"}
    tlds = (*english_tlds, "nl", "be", "cn", "de", "ga", "ru")
    email_regex = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
    syntax_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,6}")
    at_words_regex = re.compile(r"[a-zA-Z]*@[a-zA-Z]*")
    disposable_providers = [x.rstrip().lower() for x in open(PATH / "disposable_providers.txt")]
    free_providers = [x.rstrip().lower() for x in open(PATH / "free_providers.txt")]
    mongo_cache = MongoDB("cdqc.email_checker_cache")
    mongo_mx = MongoDB("cdqc.email_checker_mx_records")
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

        self.OUTPUT_DICT = {
            "accept_all": False,
            "corrected": False,
            "disposable": False,
            "domain": None,
            "email": self.EMAIL,
            "free": False,
            "language": "English",
            "mx_code": None,
            "mx_record": None,
            "qualification": None,
            "safe_to_send": True,
            "status": None,
            "success": True,
            "time": None,
            "user": None,
        }

    def parse_and_correct(self):

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

    def check_syntax(self):
        matched = self.syntax_regex.match(self.EMAIL)
        if not matched or len(self.EMAIL.strip()) > 320:
            raise ValueError

    def _connect(self, rcpt: str) -> int:

        server = SMTP(timeout=10)

        if self.DEBUG:
            server.set_debuglevel(1)
        else:
            server.set_debuglevel(0)

        server.connect(self.OUTPUT_DICT["mx_record"])

        server.helo()
        server.mail("my@from.addr.ess")
        code, _ = server.rcpt(rcpt)
        server.quit()

        return code

    def check_accept_all(self):
        if not self.CHECK_ACCEPT_ALL:
            return

        code = self._connect(f"70206294287020629428@{self.OUTPUT_DICT['domain']}")

        if code == 250:
            self.OUTPUT_DICT["accept_all"] = True
            self.OUTPUT_DICT["safe_to_send"] = False

    def check_user(self):

        code = self._connect(self.EMAIL)

        self.OUTPUT_DICT["mx_code"] = code

        if code == 250:
            self.OUTPUT_DICT["status"] = "OK"
            self.OUTPUT_DICT["qualification"] = "OK"
        else:
            self.OUTPUT_DICT["status"] = "USELESS"
            self.OUTPUT_DICT["qualification"] = f"Not Permitted ({code})"

    def parse_domain_and_country(self):
        split_address = self.EMAIL.split("@")
        self.OUTPUT_DICT["user"] = split_address[0]
        self.OUTPUT_DICT["domain"] = str(split_address[1])
        tld = split_address[1].split(".")[-1]
        if tld not in self.english_tlds:
            try:
                self.OUTPUT_DICT["language"] = Locale.parse(f"und_{tld}").language_name
            except (ValueError, UnknownLocaleError):
                pass

    def check_disposable(self):
        if self.OUTPUT_DICT["domain"].lower() in self.disposable_providers:
            self.OUTPUT_DICT["disposable"] = True
            self.OUTPUT_DICT["safe_to_send"] = False

    def check_free(self):
        if self.OUTPUT_DICT["domain"].lower() in self.free_providers:
            self.OUTPUT_DICT["free"] = True

    def time_spent(self, ret: bool = False) -> Optional[str]:
        if ret:
            return str(int((dt.now() - self.START).total_seconds() * 1000))
        else:
            self.OUTPUT_DICT["time"] = str(int((dt.now() - self.START).total_seconds() * 1000))

    def search_cache(self) -> Optional[dict]:
        if self.USE_CACHE:
            result = self.mongo_cache.find_one({
                "email": self.EMAIL,
                "created_at": {"$gte": dt.now() - self.td},
            })

            if result and "output" in result and result["output"]:
                result["output"]["time"] = self.time_spent(True)
                return result["output"]

    def save_request(self):
        if self.USE_CACHE:
            self.mongo_cache.insert_one({
                "email": self.ORIGINAL_EMAIL,
                "check_accept_all": self.CHECK_ACCEPT_ALL,
                "output": self.OUTPUT_DICT,
                "created_at": dt.now()
            })

    def get_mx_records(self):

        cached_mx_records = self.mongo_mx.find_one({"domain": self.OUTPUT_DICT["domain"]})

        if cached_mx_records:
            self.OUTPUT_DICT["mx_record"] = cached_mx_records["mx_records"][0]
        else:
            records = resolve.resolve(self.OUTPUT_DICT["domain"], "MX")
            mx_records = [record.exchange.to_text() for record in records]

            if mx_records:
                self.OUTPUT_DICT["mx_record"] = mx_records[0]

            self.mongo_mx.insert_one({
                "domain": self.OUTPUT_DICT["domain"],
                "mx_records": mx_records,
                "accept_all": False
            })

    def validate_email(self) -> dict:
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
            self.parse_domain_and_country()

            # Update output dictionary
            self.OUTPUT_DICT["email"] = self.EMAIL

            # Check if domain is from a disposable mail provider
            self.check_disposable()

            # Check if domain is from a free mail provider
            self.check_free()

            # Get MX records - Searches in mx_records collection if previously queried
            self.get_mx_records()

            # Check if user exists on mailserver
            self.check_user()

            # Check if mailserver accepts all user names
            self.check_accept_all()

            # Save the request into cache collection on Mongo
            self.save_request()

            # Update output dictionary with the request time
            self.time_spent()

            return self.OUTPUT_DICT

        except ValueError:
            if self.DEBUG:
                traceback.print_exc()
            self.OUTPUT_DICT.update({
                "email": self.EMAIL,
                "qualification": "Invalid e-mail address format",
                "safe_to_send": False,
                "status": "MANREV",
                "time": self.time_spent(True),
            })
            return self.OUTPUT_DICT

        except (DNSException, OSError, SMTPServerDisconnected):
            if self.DEBUG:
                traceback.print_exc()
            self.OUTPUT_DICT.update({
                "email": self.EMAIL,
                "qualification": "SMTP Invalid",
                "safe_to_send": False,
                "status": "USELESS",
                "time": self.time_spent(True),
            })
            return self.OUTPUT_DICT

        except Exception:  # noqa
            if self.DEBUG:
                traceback.print_exc()
            self.OUTPUT_DICT.update({
                "email": self.EMAIL,
                "qualification": "Exception",
                "safe_to_send": False,
                "status": "EXCEPTION",
                "success": False,
                "time": self.time_spent(True),
            })
            return self.OUTPUT_DICT


def validate_email(
        email: str,
        check_accept_all: bool,
        use_cache: bool,
        debug: bool = False,
):
    return _EmailValidator(
        email=email,
        check_accept_all=check_accept_all,
        use_cache=use_cache,
        debug=debug,
    ).validate_email()


@lru_cache()
def check_email(
        email: str,
        safe_to_send: bool = False,
) -> Union[dict, bool]:
    """Use Matrixian's Email Checker API to validate an email address.

    Optionally, set safe_to_send to True for boolean output.
    """
    response = get(
        f"{URL}{email}",
        text_only=True,
        timeout=30,
    )
    if safe_to_send:
        return response["safe_to_send"]
    return response
