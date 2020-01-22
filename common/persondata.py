from collections import namedtuple, MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from json import dumps, loads
from logging import info, debug
from re import sub
from socket import gethostname
from time import localtime, sleep
from typing import Iterable, NamedTuple, Optional, Tuple, Union

from dateutil.parser import parse as dateparse
from phonenumbers import is_valid_number, parse
from phonenumbers.phonenumberutil import NumberParseException
from requests import get as rget
from text_unidecode import unidecode

from .connectors import ESClient, MongoDB
from .handlers import Timer, get
from .parsers import Checks, flatten, levenshtein


class BaseDataClass(MutableMapping):
    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)


@dataclass
class Data(BaseDataClass):
    postalCode: str = None
    houseNumber: int = None
    houseNumberExt: str = None
    initials: str = None
    lastname: str = None
    telephone: int = None
    mobile: int = None
    number: int = None
    gender: str = None
    date_of_birth: datetime = None


@dataclass
class Score:
    __slots__ = [
        "source",
        "yearOfRecord",
        "deceased",
        "lastNameNumber",
        "gender",
        "dateOfBirth",
        "phoneNumberNumber",
        "occurring",
        "moved",
        "mobile",
        "matchedNames",
        "foundPersons",
    ]
    source: str
    yearOfRecord: str
    deceased: Optional[int]
    lastNameNumber: int
    gender: str
    dateOfBirth: int
    phoneNumberNumber: int
    occurring: bool
    moved: bool
    mobile: bool
    matchedNames: Tuple[str, str]
    foundPersons: int


class SourceMatch:
    def __init__(self):
        super().__init__()
        self.data = self._matched = None

    def _match_sources(self) -> Iterable:
        """Assign self._match_sources_def or
        self._match_sources_cbs to this function."""
        pass

    def _lastname_match(self, response):
        return (response.get("lastname")
                and self.data.lastname
                and (response.get("lastname") in self.data.lastname
                     or self.data.lastname in response.get("lastname"))
                ) or False

    def _initials_match(self, response):
        try:
            return (response.get("initials") and self.data.initials
                    and response.get("initials")[0] == self.data.initials[0]
                    ) or False
        except IndexError:
            return False

    def _gender_match(self, response):
        return (response.get("gender") and self.data.gender
                and response.get("gender") == self.data.gender
                ) or False

    def _address_match(self, response):
        return (response.get("address_current_postalCode")
                and self.data.postalCode
                and response.get("address_current_houseNumber")
                and self.data.houseNumber
                and response.get("address_current_postalCode") == self.data.postalCode
                and response.get("address_current_houseNumber") == self.data.houseNumber
                ) or False

    def _phone_match(self, response):
        return ((response.get("phoneNumber_mobile")
                 and self.data.mobile
                 and response.get("phoneNumber_mobile") == self.data.mobile
                 ) or (response.get("phoneNumber_number")
                       and self.data.number and
                       response.get("phoneNumber_number") == self.data.number)
                ) or False

    def _dob_match(self, response):
        return (response.get("birth_date") != "1900-01-01T00:00:00Z"
                and self.data.date_of_birth
                and response.get("birth_date") == self.data.date_of_birth.strftime("%Y-%m-%dT00:00:00Z")
                ) or False

    def _set_match(self, response: dict):
        self._matched = {
            "lastname": self._lastname_match(response),
            "initials": self._initials_match(response),
            "gender": self._gender_match(response),
            "address": self._address_match(response),
            "birth_date": self._dob_match(response),
            "phone": self._phone_match(response),
        }

    @property
    def _match_keys(self):
        return {key for key in self._matched if self._matched[key]}

    def _match_sources_def(self):
        yield "A", (self._matched["lastname"]
                    and self._matched["address"]
                    and self._matched["birth_date"]
                    and self._matched["phone"])
        yield "B", (self._matched["lastname"] and
                    ((self._matched["address"] and self._matched["birth_date"])
                     or (self._matched["birth_date"] and self._matched["phone"])
                     or (self._matched["address"] and self._matched["phone"])))
        yield "C", (self._matched["lastname"] and
                    (self._matched["address"]
                     or self._matched["phone"]
                     or self._matched["birth_date"]))
        yield "D", True

    def _match_sources_cbs(self):
        yield "N", self._matched["lastname"] and self._matched["address"]
        yield "A", self._matched["address"]

    def _get_source(self, response: dict) -> str:
        self._set_match(response)
        for source, match in self._match_sources():
            if match:
                break
        else:
            raise RuntimeError(
                "No source could be defined for this match!",
                self.data, response)
        return source


class SourceScore:
    def __init__(self):
        super().__init__()
        self._year = datetime.now().year
        self._score_testing = False

    @staticmethod
    def _categorize_score(score: float) -> int:
        """Assign self._categorize_def or self._categorize_cbs
        to this function."""
        pass

    def _calc_score(self, result_tuple: Score) -> float:
        """Calculate a quality score for the found number."""
        # Set constants
        x = 100
        year = self._year

        def date_score(result: Score, score: float) -> float:
            return (1 - ((year - int(result.yearOfRecord)) / (x / 2))) * score

        def source_score(result: Score, score: float) -> float:
            source = {
                "Kadaster_4": 1,
                "Kadaster_3": 2,
                "Kadaster_2": 3,
                "Kadaster_1": 4,
                "company_data_NL_contact": 5,
                "shop_data_nl_main": 5,
                "whitepages_nl_2019": 6,
                "whitepages_nl_2018": 7,
                "YP_CDs_Consumer_main": 8,
                "car_data_names_only_dob": 9,
                "yp_2004_2005": 10,
                "postregister_dm": 11,
                "Insolventies_main": 12,
                "GevondenCC": 13,
                "consumer_table_distinct_indexed": 14
            }
            return (1 - ((source[result.source] - 1) / (x * 2))) * score

        def death_score(result: Score, score: float) -> float:
            _x = max((year - result.dateOfBirth) / 100, .5) if result.dateOfBirth else .5
            return score if result.deceased is None else _x * score

        def n_score(result: Score, score: float) -> float:
            for var in [result.phoneNumberNumber, result.lastNameNumber]:
                score = score * (1 - ((var - 1) / x))
            return score

        def fuzzy_score(result: Score, score: float) -> float:
            """Uses name_input and name_output,
            taking into account the number of changes in name."""
            n1, n2 = result.matchedNames
            if n1 and n2:
                lev = levenshtein(n1, n2)
                if lev < .4 and (n1 not in n2 or n2 not in n1):
                    return 0
                return lev * score
            return score

        def missing_score(result: Score, score: float) -> float:
            if result.dateOfBirth is None:
                score = .9 * score
            if result.gender is None:
                score = .9 * score
            return score

        def occurring_score(result: Score, score: float) -> float:
            if result.occurring:
                return 0
            return score

        def moved_score(result: Score, score: float) -> float:
            if result.moved:
                return 0.9 * score if result.mobile else 0.2 * score
            return score

        def data_score(result: Score, score: float) -> float:
            return 0 if not result.matchedNames[0] else score

        def persons_score(result: Score, score: float) -> float:
            return (1 - ((result.foundPersons - 1) / (x / 4))) * score

        def full_score(result: Score, score: int = 1) -> Union[float, None]:
            if result is None:
                return
            for func in (persons_score,
                         data_score,
                         moved_score,
                         n_score,
                         missing_score,
                         fuzzy_score,
                         death_score,
                         source_score,
                         date_score,
                         occurring_score):
                score = func(result, score)
            return score

        # Calculate score
        score_percentage = full_score(result_tuple)
        return score_percentage

    @staticmethod
    def _categorize_def(score: float) -> int:
        if score is not None:
            if score >= 3 / 4:
                score = 1
            elif score >= 2 / 4:
                score = 2
            elif score >= 1 / 4:
                score = 3
            else:
                score = 4
        return score

    @staticmethod
    def _categorize_cbs(score: float) -> int:
        if score is not None:
            if score >= 2 / 3:
                score = 3
            elif score >= 1 / 3:
                score = 2
            else:
                score = 1
        return score

    def _convert_score(self, result_tuple: Score) -> Union[int, float]:
        score_percentage = self._calc_score(result_tuple)
        if self._score_testing:
            return score_percentage
        categorized_score = self._categorize_score(score_percentage)
        return categorized_score


class NoMatch(Exception):
    pass


class PersonData(SourceMatch, SourceScore):
    # TODO: documentation
    def __init__(self, **kwargs):
        super().__init__()

        # data holders
        self.result = self.data = None
        self._clean = Cleaner().clean

        # connectors
        self._es = ESClient("dev_peter.person_data_20190716")
        self._vn = ESClient("dev_peter.validated_numbers")
        if gethostname() == "matrixian":
            self._phone_url = "http://localhost:5000/call/"
        else:
            self._phone_url = "http://94.168.87.210:4000/call/"
        self._email_url = ("http://develop.platform.matrixiangroup.com"
                           ":4000/email?email=")
        try:
            self._local = (rget("https://api.ipify.org").text
                           == "94.168.87.210")
        except (ConnectionError, IOError):
            self._local = False

        # kwargs  # TODO: Check if you use all of these!
        self._cbs = kwargs.pop("cbs", False)
        self._email = kwargs.pop("email", False)
        self._use_id_query = kwargs.pop("id_query", False)
        self._strictness = kwargs.pop("strictness", 5)
        self._respect_hours = kwargs.pop("respect_hours", True)
        self._name_only_query = kwargs.pop("name_only_query", True)
        self._score_testing = kwargs.pop("score_testing", False)
        self._call_to_validate = kwargs.pop("call_to_validate", True)
        self._response_type = kwargs.pop("response_type", "all")
        self._use_sources = kwargs.pop("sources", ())
        categories = ("all", "name", "address", "phone")
        if (self._response_type not in categories and
                not isinstance(self._response_type, (tuple, list))):
            raise ValueError(f"Requested fields should be one"
                             f" of {', '.join(categories)}")
        if self._cbs:
            self._match_sources = self._match_sources_cbs
            self._categorize_score = self._categorize_cbs
        else:
            self._match_sources = self._match_sources_def
            self._categorize_score = self._categorize_def

        # data structures
        self._es_mapping = {
            "lastname": "lastname",
            "initials": "initials",
            "postalCode": "address.current.postalCode",
            "houseNumber": "address.current.houseNumber",
            "houseNumberExt": "address.current.houseNumberExt",
            "mobile": "phoneNumber.mobile",
            "number": "phoneNumber.number",
            "gender": "gender",
            "date_of_birth": "birth.date",
        }
        self._score_mapping = {
            "lastname": "name_score",
            "phoneNumber_mobile": "mobile_score",
            "phoneNumber_number": "number_score",
            "address_current_postalCode": "address_score",
        }

    def __repr__(self):
        return f"PersonData(in={self.data}, out={self.result})"

    @property
    def _requested_fields(self) -> tuple:
        if isinstance(self._response_type, (tuple, list)):
            return self._response_type
        elif self._response_type == "all":
            return (
                "address_current_city",
                "address_current_country",
                "address_current_houseNumber",
                "address_current_houseNumberExt",
                "address_current_location",
                "address_current_postalCode",
                "address_current_state",
                "address_current_street",
                "address_moved",
                "birth_date",
                "contact_email",
                "death_date",
                "firstname",
                "gender",
                "initials",
                "lastname",
                "middlename",
                "phoneNumber_country",
                "phoneNumber_mobile",
                "phoneNumber_number",
            ) if self._email else (
                "address_current_city",
                "address_current_country",
                "address_current_houseNumber",
                "address_current_houseNumberExt",
                "address_current_location",
                "address_current_postalCode",
                "address_current_state",
                "address_current_street",
                "address_moved",
                "birth_date",
                "death_date",
                "firstname",
                "gender",
                "initials",
                "lastname",
                "middlename",
                "phoneNumber_country",
                "phoneNumber_mobile",
                "phoneNumber_number",
            )
        elif self._response_type == "name":
            return (
                "birth_date",
                "death_date",
                "firstname",
                "gender",
                "initials",
                "lastname",
                "middlename",
            )
        elif self._response_type == "address":
            return (
                "address_current_city",
                "address_current_country",
                "address_current_houseNumber",
                "address_current_houseNumberExt",
                "address_current_location",
                "address_current_postalCode",
                "address_current_state",
                "address_current_street",
                "address_moved",
            )
        elif self._response_type == "phone":
            return (
                "phoneNumber_country",
                "phoneNumber_mobile",
                "phoneNumber_number",
            )

    @property
    def _main_fields(self) -> tuple:
        if self._response_type == "phone":
            return "phoneNumber_number", "phoneNumber_mobile"
        elif self._response_type == "address":
            return "address_current_postalCode",
        elif self._response_type == "name":
            return "lastname",
        else:
            return ("lastname", "address_current_postalCode",
                    "phoneNumber_number", "phoneNumber_mobile")

    @property
    def _queries(self) -> Tuple[str, dict]:
        """
        full: zoveel als maar kan, op should
        initial: met 4 velden, wildcard op initials, geen houseNumberExt
        name: met 3 velden, geen initials en houseNumberExt
        wildcard: zelfde, maar met wildcard op lastname
        address: met 3 velden, fuzziness en wildcard op houseNumberExt in should met minimum_should_match=1
        name_only: met lastname en initials, wildcard op lastname,
            fuzziness en wildcard op initials in should met minimum_should_match=1
        """
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {
                            self._es_mapping[field]: self.data[field]}}
                        for field in self.data
                        if field != "telephone" and self.data[field]],
                    "minimum_should_match": self._strictness,
                }
            },
            "sort": [
                {"dateOfRecord": "desc"}
            ]
        }
        if self._use_sources:
            query["query"]["bool"]["must"] = [{"terms": {"source": [*self._use_sources]}}]
        yield "full", query
        if (self.data.postalCode and self.data.houseNumber
                and self.data.lastname and self.data.initials):
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {
                                "address.current.postalCode":
                                    self.data.postalCode}},
                            {"match": {
                                "address.current.houseNumber":
                                    self.data.houseNumber}},
                            {"match": {
                                "lastname": {
                                    "query": self.data.lastname,
                                    "fuzziness": 2}}},
                            {"wildcard": {
                                "initials":
                                    f"{self.data.initials[0].lower()}*"
                            }},
                        ],
                    }
                },
                "sort": [
                    {"dateOfRecord": "desc"}
                ]
            }
            yield "initial", self._extend_query(query)
        if self.data.lastname and self.data.initials:
            if self.data.date_of_birth:
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"birth.date": self.data.date_of_birth}},
                                {"match": {"lastname": {
                                    "query": self.data.lastname,
                                    "fuzziness": 2}}},
                                {"wildcard": {
                                    "initials": f"{self.data.initials[0].lower()}*"
                                }},
                            ],
                        }
                    },
                    "sort": [
                        {"dateOfRecord": "desc"}
                    ]
                }
                yield "dob", self._extend_query(query)
            if self.data.number:
                if self.data.date_of_birth:
                    query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"phoneNumber.number": self.data.number}},
                                    {"match": {"lastname": {
                                        "query": self.data.lastname,
                                        "fuzziness": 2}}},
                                    {"wildcard": {
                                        "initials": f"{self.data.initials[0].lower()}*"
                                    }},
                                ],
                            }
                        },
                        "sort": [
                            {"dateOfRecord": "desc"}
                        ]
                    }
                    yield "number", self._extend_query(query)
            if self.data.mobile:
                if self.data.date_of_birth:
                    query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"phoneNumber.mobile": self.data.mobile}},
                                    {"match": {"lastname": {
                                        "query": self.data.lastname,
                                        "fuzziness": 2}}},
                                    {"wildcard": {
                                        "initials": f"{self.data.initials[0].lower()}*"
                                    }},
                                ],
                            }
                        },
                        "sort": [
                            {"dateOfRecord": "desc"}
                        ]
                    }
                    yield "mobile", self._extend_query(query)
        if (self.data.postalCode
                and self.data.houseNumber
                and self.data.lastname):
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {
                                "address.current.postalCode":
                                    self.data.postalCode}},
                            {"match": {
                                "address.current.houseNumber":
                                    self.data.houseNumber}},
                            {"match": {
                                "lastname": {
                                    "query": self.data.lastname,
                                    "fuzziness": 2}}},
                        ],
                    }
                }
            }
            if not self._cbs and self.data.initials:
                query["query"]["bool"]["must"].append(
                    {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}})
            yield "name", self._extend_query(query)
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {
                                "address.current.postalCode":
                                    self.data.postalCode}},
                            {"match": {
                                "address.current.houseNumber":
                                    self.data.houseNumber}},
                            {"wildcard": {
                                "lastname":
                                    f"*{self.data.lastname.split()[-1].lower()}*"
                            }},
                        ],
                    }
                },
                "sort": [
                    {"dateOfRecord": "desc"}
                ]
            }
            if not self._cbs and self.data.initials:
                query["query"]["bool"]["must"].append(
                    {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}})
            yield "wildcard", self._extend_query(query)
        if (self.data.postalCode
                and self.data.houseNumber
                and self.data.houseNumberExt):
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"address.current.postalCode": self.data.postalCode}},
                            {"match": {"address.current.houseNumber": self.data.houseNumber}},
                        ],
                        "should": [
                            {"wildcard": {
                                "address.current.houseNumberExt": f"*{self.data.houseNumberExt[0].lower()}*"}},
                            {"match": {"address.current.houseNumberExt": {
                                "query": self.data.houseNumberExt[0], "fuzziness": 2}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "sort": [
                    {"dateOfRecord": "desc"}
                ]
            }
            yield "address", self._extend_query(query)
        if self._name_only_query and self.data.lastname and self.data.initials:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"wildcard": {"lastname": f"*{self.data.lastname.split()[-1].lower()}*"}},
                            {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}},
                        ],
                    }
                },
                "sort": [
                    {"dateOfRecord": "desc"}
                ]
            }
            yield "name_only", self._extend_query(query)

    def _extend_query(self, query):
        if self._use_sources:
            query["query"]["bool"]["must"].append({"terms": {"source": [*self._use_sources]}})
        return query

    @staticmethod
    def _id_query(responses: list) -> dict:
        """Take a list of match responses, and return a query
        that will search for the ids of those responses."""

        def ordered_set(seq, k="id"):
            seen = set()
            seen_add = seen.add
            return [x[k] for x in seq if not (x[k] in seen or seen_add(x[k]))]

        return {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"id": _id}}
                        for _id in ordered_set(responses)],
                    "minimum_should_match": 1
                }
            },
            "sort": [
                {"dateOfRecord": "desc"}
            ]
        }

    @staticmethod
    def _check_country(country: str):
        if (country not in
                {"nederland", "netherlands", "nl", "nld"}):
            raise NotImplementedError(
                f"Not implemented for country "
                f"{country}.")

    def _check_match(self, key: str):
        """Matches where we found a phone number,
        but the phone number occurs more recently
        on another address, or with another lastname,
        should get a lower score."""
        occurring = False
        if "phoneNumber" in key:
            response = self._es.find({
                "query": {
                    "bool": {
                        "must": [
                            {"match": {key.replace("_", "."): self.result[key]}}]
                    }},
                "sort": [
                    {"dateOfRecord": "desc"}
                ]}, size=1)
            if response:
                response = response[0]
                if self._responses[key]["_id"] != response["_id"]:
                    occurring = True
        return occurring

    def _find(self):
        debug("Data = %s", self.data)
        self.result = {}
        self._responses = {}
        for _type, q in self._queries:
            if self._use_id_query:
                # TODO: test if this specifically behaves as intended
                responses = [{"_id": d["_id"], **d["_source"]}
                             for d in self._es.find(
                        self._id_query(self._es.find(
                            q, source_only=True)))]
            else:
                responses = [{"_id": d["_id"], **d["_source"]}
                             for d in self._es.find(q)]
            for response in responses:
                response = flatten(response)
                for key in self._requested_fields:
                    if key not in self.result and response[key]:
                        # t = Timer()
                        if (key in ("phoneNumber_number", "phoneNumber_mobile")
                                and not self._phone_valid(response[key])):
                            # debug("Validating key %s took %s", key, t.end())
                            continue
                        if (key in ("address_moved", "birth_date", "death_date")
                                and response[key] == "1900-01-01T00:00:00Z"):
                            # debug("Validating key %s took %s", key, t.end())
                            continue
                        if (key == "contact_email" and
                                not self._email_valid(response[key])):
                            # debug("Validating key %s took %s", key, t.end())
                            continue
                        # debug("Validating key %s took %s", key, t.end())
                        self.result[key] = response[key]
                        if key in self._main_fields:
                            self._responses[key] = response
                        self.result["search_type"] = _type
                        self.result["source"] = response["source"]
                        self.result["date"] = response["dateOfRecord"]
                if all(map(self.result.get, self._main_fields)):
                    return

    def _phone_valid(self, number: int):
        """Don't call between 22PM and 8AM; if the
        script is running then, just pause it."""
        phone = f"+31{number}"
        valid = is_valid_number(parse(phone, "NL"))
        if f"{number}".startswith(("8", "9")):
            valid = False
        if valid:
            if self._local:
                query = {"query": {"bool": {"must": [{"match": {"phoneNumber": number}}]}}}
                result = self._vn.find(query=query, first_only=True)
                if result:
                    return result["valid"]
            if self._respect_hours:
                t = localtime().tm_hour
                while t >= 22 or t < 8:
                    sleep(60)
                    t = localtime().tm_hour
            if self._call_to_validate:
                while True:
                    response = get(f"{self._phone_url}{phone}",
                                   # headers={"Connection": "close"},
                                   auth=("datateam", "matrixian"))
                    if not response.ok:
                        self._phone_url = "http://94.168.87.210:4000/call/"
                    else:
                        valid = loads(response.text)
                        # response.close()
                        break
        return valid

    def _email_valid(self, email: str):
        """Check validity of email address."""
        try:
            return get(f"{self._email_url}{email}",
                       text_only=True,
                       timeout=10
                       )["status"] == "OK"
        except Exception as e:
            debug("Exception: %s: %s", email, e)
            return False

    def _get_score(self):
        self.result["match_keys"] = set()
        if not [key for key in self._main_fields if key in self._responses]:
            raise NoMatch
        for key in self._main_fields:
            if key in self._responses:
                response = self._responses[key]
                source = self._get_source(response)
                self.result["match_keys"].update(self._match_keys)
                score = self._convert_score(Score(
                    source=response["source"],
                    yearOfRecord=response["dateOfRecord"][:4],
                    deceased=response["death_year"],
                    lastNameNumber=len(set(d["lastname"] for d in self._responses.values())),
                    gender=response["gender"],
                    dateOfBirth=response["birth_year"],
                    phoneNumberNumber=len(set(d[key] for d in self._responses.values()))
                    if "phoneNumber" in key else 1,
                    occurring=self._check_match(key),
                    moved=response["address_moved"] != "1900-01-01T00:00:00Z",
                    mobile="mobile" in key or "lastname" in key,
                    matchedNames=(self.data.lastname, response["lastname"]),
                    foundPersons=len({response["id"] for response in self._responses.values()}),
                ))
                self.result[self._score_mapping[key]] = f"{source}{score}"

    def _finalize(self):
        # Get match keys
        self._set_match(self.result)
        self.result["match_keys"].update(self._match_keys)

        # Fix dates
        self.result["date"] = dateparse(self.result["date"], ignoretz=True)
        for key in ("address_moved", "birth_date", "death_date"):
            if key in self.result:
                self.result[key] = dateparse(self.result[key], ignoretz=True)

        debug("Result = %s", self.result)

    def match(self, data: dict) -> dict:
        self._check_country(data.pop("country", "nl").lower())
        self.data = self._clean(Data(**data))
        self._find()
        if self.result:
            self._get_score()
            self._finalize()
            return self.result
        else:
            raise NoMatch


class PersonMatch:
    """Base Match class"""

    def __init__(self, strictness: int = 3, validate_phone: bool = False):
        self.drop_fields = {"source", "previous", "common", "country", "place", "fax", "id", "year",
                            "postalCodeMin", "purposeOfUse", "latitude", "longitude", "title", "state", "dateOfRecord"}
        self.title_data = NamesData.titles()
        self.data = {}
        self.result = {}
        self.results = []
        self.query = {"query": {"bool": {}}}
        self._strictness = strictness
        self.validate_phone = validate_phone
        self.es = ESClient()
        self.vn = MongoDB("dev_peter.validated_numbers")
        self.url = "http://localhost:5000/call/+31"

    def match(self, data: dict, strictness: int = None):
        t = Timer()
        if not strictness:
            strictness = self._strictness
        self.data = data
        debug("Data = %s", data)
        self.build_query(strictness=strictness)
        self.find_match()
        if self.result:
            info("End time = %s", t.end())
            return self.result
        else:
            self.build_query(strictness=strictness, fuzzy=True)
            self.find_match()
            info("End time = %s", t.end())
            return self.result

    def build_query(self, strictness: int, fuzzy: bool = False):

        if self.data.get("lastname"):
            must = {"must": [
                {"match": {"lastname": self._clean_lastname(self.data["lastname"])}},
            ]} if not fuzzy else {"must": [
                {"match": {"lastname": {"query": self._clean_lastname(self.data["lastname"]), "fuzziness": 1}}},
            ]}
            self.query["query"]["bool"] = must

        should_list = []
        if self.data.get("initials"):
            should_list.append({"match": {"initials": self._clean_initials(self.data["initials"])}})
        if self.data.get("gender"):
            should_list.append({"match": {"gender": self._clean_gender(self.data["gender"])}})
        if self.data.get("date_of_birth"):
            should_list.append({"match": {"birth.date": self._clean_dob(self.data["date_of_birth"])}})
        if self.data.get("postalCode"):
            should_list.append({"match": {"address.current.postalCode": self.data["postalCode"]}})
        if self.data.get("houseNumber"):
            should_list.append({"match": {"address.current.houseNumber": self.data["houseNumber"]}})
        if self.data.get("houseNumberExt"):
            should_list.append({"match": {"address.current.houseNumberExt": self.data["houseNumberExt"]}})
        if self.data.get("telephone") and self.data["telephone"] != "0":
            phone_number, phone_type = self._clean_phone(self.data["telephone"])
            should_list.append({"match": {f"phoneNumber.{phone_type}": phone_number}})
        if should_list:
            self.query["query"]["bool"]["should"] = should_list
            # noinspection PyTypeChecker
            self.query["query"]["bool"]["minimum_should_match"] = min(strictness, len(should_list))
        debug("query = %s", self.query)

    def find_match(self):
        # Find all matches
        self.results = self.es.find(self.query, source_only=True, size=10, sort="dateOfRecord:desc")
        if not self.results:
            info("NoMatch")
            raise NoMatch
        if self.validate_phone:
            t = Timer()
            self._validate_phone()
            debug("Validated phone numbers in %s", t.end())

        # Get the most recent match but update missing data
        info("Number of results: %s", len(self.results))
        self.result = self.results.pop(0)
        debug("Raw result: %s", self.result)
        self._update_result()
        debug("Updated result: %s", self.result)

        # Run twice to flatten nested address dicts
        self._flatten_result()
        self._flatten_result()
        debug("Final result: %s", self.result)

    def _clean_lastname(self, name: str) -> str:
        """Keep only letters; hyphens become spaces. Remove all special characters and titles"""
        if name:
            name = unidecode(sub(r"[^\sA-Za-z\u00C0-\u017F]", "", sub(r"-", " ", name)).strip()) \
                .replace("÷", "o").replace("y", "ij")
            if name != "" and name.split()[-1].lower() in self.title_data:
                name = " ".join(name.split()[:-1])
        return name

    @staticmethod
    def _clean_initials(initials: str) -> str:
        return sub(r"[^A-Za-z\u00C0-\u017F]", "", initials.upper())

    @staticmethod
    def _clean_gender(gender: str) -> str:
        return gender.upper().replace("MAN", "M").replace("VROUW", "V") if gender in [
            "Man", "Vrouw", "MAN", "VROUW", "man", "vrouw", "m", "v", "M", "V"] else ""

    @staticmethod
    def _clean_dob(dob: str) -> datetime:
        return dateparse(dob)

    @staticmethod
    def _clean_phone(phone: Union[str, int]) -> (int, str):
        if isinstance(phone, str):
            phone = phone.replace("-", "").replace(" ", "").lstrip("0")
        phone_type = "mobile" if f"{phone}".startswith("6") else "number"
        return int(phone), phone_type

    def _validate_phone(self):
        for result in self.results:
            for phone_type in ["number", "mobile"]:
                number = result["phoneNumber"][phone_type]
                if number:
                    result = self.vn.find_one({"phoneNumber": number},
                                              {"_id": False, "valid": True})
                    if result:
                        valid = result["valid"]
                    else:
                        while True:
                            valid = get(f"{self.url}{number}",
                                        auth=("datateam", "matrixian"))
                            if not valid:
                                self.url = "http://94.168.87.210:4000/call/+31"
                            else:
                                valid = loads(valid.text)
                                break
                    if not valid:
                        result["phoneNumber"]["number"] = None
        return self.results

    def _update_result(self):
        for field in {"valid", "extra"}:
            self.result["phoneNumber"].pop(field)
        for result in self.results:
            for k1, v1 in self.result.items():
                if isinstance(v1, dict) and k1 != "address":
                    for k2, v2 in v1.items():
                        if isinstance(v2, dict):
                            for k3, v3 in v2.items():
                                if not v3:
                                    self.result[k1][k2][k3] = result[k1][k2][k3]
                        elif not v2:
                            self.result[k1][k2] = result[k1][k2]
                elif not v1:
                    self.result[k1] = result[k1]

    def _flatten_result(self):
        new_result = {}
        for key, maybe_nested in self.result.items():
            if not key.startswith("_") and key not in self.drop_fields:
                if isinstance(maybe_nested, dict):
                    for subkey, value in maybe_nested.items():
                        if key in {"birth", "death"} and subkey == "date":
                            subkey = f"date_of_{key}"
                        if subkey not in self.drop_fields:
                            new_result[subkey] = value
                else:
                    new_result[key] = maybe_nested
        self.result = new_result


class PhoneNumberFinder:
    """Class for phone number enrichment."""

    def __init__(self, **kwargs):
        """Class for phone number enrichment.
        Also take a look at this class's find method.

        The find method returns a dictionary with 3 keys:
            number, source, and score.
                source can be N (name) or A (address)
                score can be 1, 2 or 3 (3 is the highest)

        If you use n=2 as argument in .find(), the keys will be:
            mobile, source, and score.

        Examples:
            pnf = PhoneNumberFinder()
            pnf.load({
                "initials": "P",
                "lastname": "Saalbrink",
                "postalCode": "1071XB",
                "houseNumber": "71",
                "houseNumberExt": "B",
            })
            pnf.find()
            :return: {'number': 649978891, 'source': 'N', 'score': 3}

            pnf = PhoneNumberFinder()
            pnf.load({
                "initials": "P",
                "lastname": "Saalbrink",
                "postalCode": "1071XB",
                "houseNumber": "71",
                "houseNumberExt": "B",
            })
            pnf.find(n=2)
            :return:
                {'mobile': {'number': 649978891, 'source': 'N', 'score': 3},
                'number': {'number': 203345554, 'source': 'N', 'score': 1}}
        """
        self.Data = namedtuple("Data", [
            "postalCode",
            "houseNumber",
            "houseNumberExt",
            "initials",
            "lastname"
        ])
        self.Score = namedtuple("Score", [
            "source",
            "yearOfRecord",
            "deceased",
            "lastNameNumber",
            "gender",
            "dateOfBirth",
            "phoneNumberNumber",
            "isFuzzy",
            "occurring",
            "moved",
            "mobile",
            "matchedNames",
        ])
        self.year = datetime.now().year
        self.url = "http://localhost:5000/call/+31"

        self.es = ESClient()
        self.vn = MongoDB("dev_peter.validated_numbers")
        self.local = rget('https://api.ipify.org').text == "94.168.87.210"

        self.data = None
        self.queries = {}
        self.result = {}

        self.respect_hours = kwargs.pop("respect_hours", True)
        self.name_only = kwargs.pop("name_only_query", False)
        self.score_testing = kwargs.pop("score_testing", False)
        self.call_to_validate = kwargs.pop("call_to_validate", True)
        self.save_matching_records = kwargs.pop("save_matching_records", False)

        if self.save_matching_records:
            with open(self.save_matching_records) as f:
                self.matching_numbers = {int(n) for n in f.read().split(",")}
            self.save_matching_records = f"{self.save_matching_records}_records"
        else:
            self.matching_numbers = set()

        self.query_types = [
            "initial", "name", "fuzzy", "address", "name_only"
        ] if self.name_only else [
            "initial", "name", "fuzzy", "address"
        ]

    def __repr__(self):
        return f"PhoneNumberFinder(result={self.result})"

    def __str__(self):
        return f"PhoneNumberFinder(result={self.result})"

    def match(self, data: Union[MutableMapping, NamedTuple], n: int = 1):
        t = Timer()
        self.load(data=data)
        debug("Data = %s", self.data)
        debug("Queries = %s", self.queries)
        result = self.find(n=n)
        debug("Result = %s", self.result)
        info("End time = %s", t.end())
        return result

    def load(self, data: Union[MutableMapping, NamedTuple], a: str = "address.current."):
        """Load queries from data. Used keys:
        initials, lastname, postalCode, houseNumber, houseNumberExt"""

        # Start clean
        self.data = None
        self.queries = {}
        self.result = {}

        # Load data
        self.data = self.Data(
            Checks.str_or_empty(data.get("postalCode")),
            Checks.int_or_null(Checks.float_or_null(data.get("houseNumber"))),
            Checks.str_or_empty(data.get("houseNumberExt")),
            Checks.str_or_empty(data.get("initials")).replace(".", ""),
            Checks.str_or_empty(data.get("lastname"))
        ) if isinstance(data, MutableMapping) else data

        if not ((self.data.initials and self.data.lastname)
                or (self.data.postalCode and self.data.houseNumber)):
            raise NoMatch("Not enough data to match on.")

        # Store one ES query for each search type
        if self.data.postalCode and self.data.houseNumber:
            q = [{"match": {f"{a}houseNumber": self.data.houseNumber}},
                 {"match": {f"{a}postalCode": self.data.postalCode}}]
            if self.data.houseNumberExt:
                q.append({"match": {f"{a}houseNumberExt": self.data.houseNumberExt}})
            self.queries["address"] = {"query": {"bool": {"must": q}}}

            if self.data.lastname:
                q = [{"match": {f"{a}houseNumber": self.data.houseNumber}},
                     {"match": {f"{a}postalCode": self.data.postalCode}},
                     {"match": {"lastname": {"query": self.data.lastname, "fuzziness": 2}}}]
                if self.data.houseNumberExt:
                    q.append({"match": {f"{a}houseNumberExt": {"query": self.data.houseNumberExt, "fuzziness": 2}}})
                self.queries["fuzzy"] = {"query": {"bool": {"must": q}}}

                q = [{"match": {f"{a}houseNumber": self.data.houseNumber}},
                     {"match": {f"{a}postalCode": self.data.postalCode}},
                     {"match": {"lastname": self.data.lastname}}]
                if self.data.houseNumberExt:
                    q.append({"match": {f"{a}houseNumberExt": self.data.houseNumberExt}})
                self.queries["name"] = {"query": {"bool": {"must": q}}}

                if self.data.initials:
                    q = [{"match": {f"{a}houseNumber": self.data.houseNumber}},
                         {"match": {f"{a}postalCode": self.data.postalCode}},
                         {"match": {"lastname": self.data.lastname}},
                         {"match": {"initials": self.data.initials}}]
                    if self.data.houseNumberExt:
                        q.append({"match": {f"{a}houseNumberExt": self.data.houseNumberExt}})
                    self.queries["initial"] = {"query": {"bool": {"must": q}}}

        if self.name_only and self.data.lastname and self.data.initials:
            q = [{"wildcard": {"lastname": f"*{self.data.lastname.lower()}"}},
                 {"match": {"initials": {"query": self.data.initials}}}]
            self.queries["name_only"] = {"query": {"bool": {"must": q}}}

    @staticmethod
    def sleep_or_continue():
        """Don't call between 22PM and 8AM; if the
        script is running then, just pause it."""
        t = localtime().tm_hour
        while t >= 22 or t < 8:
            sleep(60)
            t = localtime().tm_hour

    def validate(self, phone: str) -> bool:
        """Offline (i.e., very basic) and online (i.e., paid) phone
        number validation.

        :raises:
            ConnectionError: On API error.
            JSONDecodeError: On API error.
            NumberParseException: On parse error.
            ValueError: On parse error."""
        t = Timer()
        # Before calling our API, do basic (offline) validation
        valid = is_valid_number(parse(phone, "NL"))
        if phone.startswith(("8", "9")):
            valid = False
        if valid:
            if self.local:
                result = self.vn.find_one({"phoneNumber": int(phone)}, {"_id": False, "valid": True})
                debug("MongoDB lookup: %s", t.end())
                if result:
                    return result["valid"]
            if self.respect_hours:
                self.sleep_or_continue()
            if self.call_to_validate:
                while True:
                    valid = get(f"{self.url}{phone}", auth=("datateam", "matrixian"))
                    if not valid:
                        self.url = "http://94.168.87.210:4000/call/+31"
                        debug("First try: %s", t.end())
                    else:
                        valid = loads(valid.text)
                        debug("Request: %s", t.end())
                        break
        return valid

    def extract_number(self, data, records, record, result, source, score, fuzzy, number_types: list = None):
        t = Timer()
        for number_type in number_types:
            number = record["phoneNumber"][number_type]
            if result is None and number is not None:
                debug("Before validating: %s", t.end())
                if self.validate(f"{number}"):
                    debug("After validating: %s", t.end())
                    result = number
                    source = "N" if record["lastname"] and (
                            record["lastname"] in data.lastname
                            or data.lastname in record["lastname"]) else "A"
                    score = self.calculate_score(self.Score(
                        record["source"],
                        record["dateOfRecord"][:4],
                        record["death"]["year"],
                        len(set(d["lastname"] for d in records)),
                        record.get("gender"),
                        record["birth"]["year"],
                        len(set(d["phoneNumber"]["number"] for d in records)),
                        fuzzy,
                        self.es.query(field=f"phoneNumber.{number_type}",
                                      value=int(number), size=0)["hits"]["total"],
                        record["address"]["moved"] != "1900-01-01T00:00:00Z",
                        number_type == "mobile",
                        (data.lastname, record["lastname"])
                    ))
                debug("Total for %s: %s", number_type, t.end())
        debug("Took %s, result: %s", t.end(), result)
        return result, source, score

    def get_result(self, data, records, fuzzy: bool, result=None, source=None, score=None, number_types: list = None):
        t = Timer()
        if not number_types:
            number_types = ["number", "mobile"]
        # Loop over the results for a query, from recent to old
        for record in records:
            # Get fixed and mobile number from records (most recent first) and perform validation
            result, source, score = self.extract_number(
                data, records, record, result, source, score, fuzzy, number_types)
            if result:
                debug("Got result in: %s", t.end())
                if self.save_matching_records:
                    if result in self.matching_numbers:
                        with open(self.save_matching_records, "a", encoding="utf-8") as f:
                            f.write(f"{dumps(record, ensure_ascii=False)},\n")
                break
        else:
            debug("Got no result in: %s", t.end())
        return result, source, score

    def calculate_score(self, result_tuple: namedtuple) -> float:
        """Calculate a quality score for the found number."""
        # Set constants
        x = 100
        year = self.year

        def date_score(result: namedtuple, score: float) -> float:
            return (1 - ((year - int(result.yearOfRecord)) / (x / 2))) * score

        def source_score(result: namedtuple, score: float) -> float:
            source = {
                "Kadaster_4": 1,
                "Kadaster_3": 2,
                "Kadaster_2": 3,
                "Kadaster_1": 4,
                "company_data_NL_contact": 5,
                "shop_data_nl_main": 5,
                "whitepages_nl_2019": 6,
                "whitepages_nl_2018": 7,
                "YP_CDs_Consumer_main": 8,
                "car_data_names_only_dob": 9,
                "yp_2004_2005": 10,
                "postregister_dm": 11,
                "Insolventies_main": 12,
                "GevondenCC": 13,
                "consumer_table_distinct_indexed": 14
            }
            return (1 - ((source[result.source] - 1) / (x * 2))) * score

        def death_score(result: namedtuple, score: float) -> float:
            _x = max((year - result.dateOfBirth) / 100, .5) if result.dateOfBirth else .5
            return score if result.deceased is None else _x * score

        def n_score(result: namedtuple, score: float) -> float:
            for var in [result.phoneNumberNumber, result.lastNameNumber]:
                score = score * (1 - ((var - 1) / x))
            return score

        def fuzzy_score(result: namedtuple, score: float) -> float:
            """Uses name_input and name_output, before and after fuzzy
            matching, taking into account the number of changes in name."""

            return levenshtein(*result.matchedNames) * score if result.isFuzzy else score

        def missing_score(result: namedtuple, score: float) -> float:
            if result.dateOfBirth is None:
                score = .9 * score
            if result.gender is None:
                score = .9 * score
            return score

        def occurring_score(result: namedtuple, score: float) -> float:
            """This should be the last step."""
            return min(score * (1 + ((result.occurring - 1) / x)), 1)

        def moved_score(result: namedtuple, score: float) -> float:
            if result.moved:
                return 0.9 * score if result.mobile else 0.2 * score
            else:
                return score

        def data_score(result: namedtuple, score: float) -> float:
            return 0 if not result.matchedNames[0] else score

        def full_score(result: namedtuple, score: int = 1) -> Union[float, None]:
            if result is None:
                return
            for func in [data_score,
                         moved_score,
                         n_score,
                         missing_score,
                         fuzzy_score,
                         death_score,
                         source_score,
                         date_score,
                         occurring_score]:
                score = func(result, score)
            return score

        def categorize(score: float):
            if score is not None:
                if score >= 2 / 3:
                    score = 3
                elif score >= 1 / 3:
                    score = 2
                else:
                    score = 1
            return score

        # Calculate score
        score_percentage = full_score(result_tuple)
        if self.score_testing:
            return score_percentage
        categorized_score = categorize(score_percentage)
        return categorized_score

    def find(self, n: int = 1) -> dict:
        """Download matching records from ES,
        and extract phone numbers.
        Also take a look at this class's __init__ method.

        The strategy for this follows CBS protocol:
        1. Enrich fixed number based on address
            and (if available) name match
        2. Enrich mobile number based on address
            and (if available) name match
        3. Enrich fixed number based on address match
        4. Enrich mobile number based on address match
        5. Enrich fixed number based on fuzzy address
            (and, if available, name) match
        6. Enrich mobile number based on fuzzy address
            (and, if available, name) match

        If one step results in a phone number, the number is
        verified and returned; the remaining steps are skipped.

        If n=1, returns a dict with the best number.
        If n=2, returns a dict with two dicts."""

        t = Timer()

        if n not in {1, 2}:
            raise ValueError("n=1 will return any number and n=2 will"
                             " return fixed and mobile; pick either.")

        # Get the ES response for each query
        for q in self.query_types:
            query = self.queries.get(q)
            if query:
                response = self.es.find(query, size=10,
                                        source_only=True,
                                        sort="dateOfRecord:desc")
                if response:
                    debug("Response %s query: %s", q, response)
                    if n == 1:
                        result, source, score = self.get_result(
                            self.data, response, q in {"fuzzy", "name_only"})
                        if result:
                            self.result = {
                                "number": result,
                                "source": source,
                                "score": score,
                            }
                            break
                    elif n == 2:
                        for number_type in ["number", "mobile"]:
                            result, source, score = self.get_result(
                                self.data, response, q in {"fuzzy", "name_only"},
                                number_types=[number_type])
                            if result:
                                if number_type not in self.result:
                                    self.result[number_type] = {
                                        "number": result,
                                        "source": source,
                                        "score": score,
                                    }
                        if self.result.get("number") and self.result.get("mobile"):
                            break
            debug("Elapsed time after %s query: %s", q, t.end())
        # noinspection PyUnboundLocalVariable
        info("Elapsed time after %s query: %s", q, t.end())

        # Return the found phone numbers for this query
        return self.result


class Cleaner:
    def __init__(self):
        self.data = {}
        self.title_data = NamesData.titles()

    def clean(self, data: Union[Data, dict]) -> Union[Data, dict]:
        self.data = data
        for function in [function for function in dir(self)
                         if function.startswith("_clean")]:
            with suppress(KeyError):
                self.__getattribute__(function)()
        return self.data

    def _clean_phones(self):
        for _type in ("number", "telephone", "mobile"):
            if _type in self.data:
                if isinstance(self.data[_type], str):
                    # Clean number
                    for s in (".0", "+31"):
                        self.data[_type] = self.data[_type].replace(s, "")
                    self.data[_type] = sub(r"[^0-9]", "", self.data[_type])
                    self.data[_type] = self.data[_type].lstrip("0")
                    if self.data[_type]:
                        self.data[_type] = int(self.data[_type])
                    else:
                        self.data.pop(_type)
                # Check format and syntax
                try:
                    number_valid = is_valid_number(parse(f"+31{self.data[_type]}", "NL"))
                except NumberParseException:
                    number_valid = False
                if not number_valid:
                    self.data.pop(_type)
                elif _type == "telephone":
                    if f"{self.data[_type]}".startswith("6"):
                        self.data["mobile"] = self.data.pop(_type)
                    else:
                        self.data["number"] = self.data.pop(_type)

    def _clean_initials(self):
        if isinstance(self.data["initials"], str):
            self.data["initials"] = sub(r"[^A-Za-z\u00C0-\u017F]", "",
                                        self.data["initials"].upper())
        if not self.data["initials"]:
            self.data.pop("initials")

    def _clean_gender(self):
        if isinstance(self.data["gender"], str) and self.data["gender"] in (
                "Man", "Vrouw", "MAN", "VROUW", "man", "vrouw", "m", "v", "M", "V"):
            self.data["gender"] = self.data["gender"].upper().replace("MAN", "M").replace("VROUW", "V")
        else:
            self.data.pop("gender")

    def _clean_lastname(self):
        """Keep only letters; hyphens become spaces.
        Remove all special characters and titles."""
        if isinstance(self.data["lastname"], str):
            self.data["lastname"] = self.data["lastname"].title()
            self.data["lastname"] = sub(r"-", " ", self.data["lastname"])
            self.data["lastname"] = sub(r"[^\sA-Za-z\u00C0-\u017F]", "", self.data["lastname"])
            self.data["lastname"] = unidecode(self.data["lastname"].strip())
            self.data["lastname"] = self.data["lastname"].replace("÷", "o").replace("y", "ij")
            if self.data["lastname"] and self.data["lastname"].split()[-1].lower() in self.title_data:
                self.data["lastname"] = " ".join(self.data["lastname"].split()[:-1])
        if not self.data["lastname"]:
            self.data.pop("lastname")

    def _clean_dob(self):
        if self.data["date_of_birth"] and isinstance(self.data["date_of_birth"], str):
            self.data["date_of_birth"] = self.data["date_of_birth"].split()[0]
            try:
                self.data["date_of_birth"] = dateparse(self.data["date_of_birth"], ignoretz=True)
            except ValueError:
                self.data["date_of_birth"] = None
        if not self.data["date_of_birth"]:
            self.data.pop("date_of_birth")

    def _clean_pc(self):
        if isinstance(self.data["postalCode"], str):
            self.data["postalCode"] = self.data["postalCode"].replace(" ", "").upper()
            if len(self.data["postcode"]) != 6:
                self.data.pop("postalCode")
        else:
            self.data.pop("postalCode")

    def _clean_hn(self):
        if isinstance(self.data["houseNumber"], str):
            for d in ("/", "-"):
                if d in self.data["houseNumber"]:
                    self.data["houseNumber"] = self.data["houseNumber"].split(d)[0]
            self.data["houseNumber"] = sub(r"[^0-9]", "", self.data["houseNumber"])
        if not self.data["houseNumber"] or self.data["houseNumber"] == "nan":
            self.data.pop("houseNumber")
        else:
            self.data["houseNumber"] = int(float(self.data["houseNumber"]))
        if not self.data["houseNumber"]:
            self.data.pop("houseNumber")

    def _clean_hne(self):
        if isinstance(self.data["houseNumberExt"], str):
            self.data["houseNumberExt"] = sub(r"[^A-Za-z0-9\u00C0-\u017F]", "",
                                              self.data["houseNumberExt"].upper())
            self.data["houseNumberExt"] = sub(r"\D+(?=\d)", "", self.data["houseNumberExt"])
        if not self.data["houseNumberExt"]:
            self.data.pop("houseNumberExt")


class NamesData:
    @staticmethod
    def first_names() -> dict:
        """Import a file with first names and gender occurrence, and return a {first_name: gender} dictionary.

        This function returns if any given Dutch first name has more male or female bearers. If the number is equal,
        None is returned. Names are cleaned before output.

        The output can be used to fill missing gender data."""
        return {doc["firstname"]: doc["gender"] for doc in
                MongoDB("dev_peter.names_data").find({"data": "firstnames"}, {"firstname": True, "gender": True})}

    @staticmethod
    def titles() -> set:
        """Imports a file with titles and returns them as a set. The output can be used to clean last name data."""
        with suppress(ConnectionError, IOError):
            if rget("https://api.ipify.org").text == "94.168.87.210":
                return set(doc["title"] for doc in
                           MongoDB("dev_peter.names_data").find({"data": "titles"}, {"title": True}))
        return {"ac", "ad", "ba", "bc", "bi", "bsc", "d", "de", "dr", "drs", "het", "ing",
                "ir", "jr", "llb", "llm", "ma", "mr", "msc", "o", "phd", "sr", "t", "van"}

    @staticmethod
    def surnames(common_only: bool = True) -> set:
        """Imports a database with surnames frequencies and returns common surnames as a list.

        Only surnames that occur more than 200 times in the Netherlands are regarded as common.
        If a name occurs twice in the file, the largest number is taken.

        The output can be used for data and matching quality calculations."""
        db = MongoDB("dev_peter.names_data")
        if common_only:
            # Return only names that occur commonly
            names_data = set(doc["surname"] for doc in
                             db.find({"data": "surnames", "number": {"$gt": 200}}, {"surname": True}))
        else:
            names_data = set(doc["surname"] for doc in db.find({"data": "surnames"}, {"surname": True}))
        return names_data
