"""Module for querying Matrixian's person database.

This module contains classes that accept some input and then return any
matching records from Matrixian's person database. The main entry point
for this is the `match()` method of the `PersonData()` class. This is
also the main method used by the `PersonChecker` of the CDQC product
(see the `data_team_validators` repository).

Additionally, there is a `NamesData` class which provides some methods
for easy access to data on several names statistics, such as last name
occurrence in the Netherlands.

Other useful objects include the `Data` class, which can serve as a
container for person data, and the `Cleaner` class, which cleans said
data.

.. py:class:: common.persondata.Cleaner


.. py:class:: common.persondata.Data
   Dataclass for person input.

.. py:class:: common.persondata.NamesData


.. py:class:: common.persondata.PersonData


.. py:class:: common.persondata.Score
   Dataclass for score calculation.

"""

from collections.abc import MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from json import loads
from logging import debug
from re import sub
from socket import gethostname
from time import localtime, sleep
from typing import Optional, Sequence, Tuple, Union

from dateutil.parser import parse as dateparse
from phonenumbers import is_valid_number, parse as phoneparse
from phonenumbers.phonenumberutil import NumberParseException
from requests.exceptions import RetryError
from text_unidecode import unidecode
from urllib3.exceptions import MaxRetryError

from .connectors.mx_elastic import ESClient, ElasticsearchException
from .exceptions import MatchError, NoMatch
from .parsers import flatten, levenshtein
from .requests import get

PD_INDEX = "dev_peter.person_data_20190716"
VN_INDEX = "dev_peter.validated_numbers"


class BaseDataClass(MutableMapping):
    """Base class for extending dataclasses
    with dictionary-like functionality.
    """
    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        self.__dict__[key] = None

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)


@dataclass
class Data(BaseDataClass):
    """Dataclass for person input."""
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
# TODO: convert to pydantic.BaseModel


@dataclass
class Score:
    """Dataclass for score calculation."""
    __slots__ = [
        "source",
        "year_of_record",
        "deceased",
        "lastname_number",
        "gender",
        "date_of_birth",
        "phonenumber_number",
        "occurring",
        "moved",
        "mobile",
        "matched_names",
        "found_persons",
    ]
    source: str
    year_of_record: str
    deceased: Optional[int]
    lastname_number: int
    gender: str
    date_of_birth: int
    phonenumber_number: int
    occurring: bool
    moved: bool
    mobile: bool
    matched_names: Tuple[str, str]
    found_persons: int
# TODO: incorporate into SourceScore:
#  total number of search results
#  frequency of lastname


class SourceMatch:
    """Calculates the "certainty" part of the output score.

    This score will be a letter ranging from A (best) to D (worst).
    It is calculated by comparing several keys of the output to the
    input.
    The main entry point is the `_get_source()` method.
    """

    def __init__(self):
        super().__init__()
        self.data = self._matched = None
        self._source_match = {5: "A", 4: "A", 3: "B", 2: "C", 1: "D"}

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

    def _match_sources(self) -> str:
        # TODO: incorporate gender into match scoring system
        keys = ("lastname", "initials", "address", "birth_date", "phone")
        values = (self._matched[m] for m in keys)
        matches = sum(bool(v) for v in values)
        return self._source_match[matches]

    def _get_source(self, response: dict) -> str:
        self._set_match(response)
        try:
            source = self._match_sources()
        except KeyError as e:
            if e.args[0] == 0:
                raise NoMatch
            raise MatchError(
                "No source could be defined for this match!",
                self.data, response) from e
        return source


class SourceScore:
    """Calculates the "quality" part of the output score.

    This score will be a numberer ranging from 1 (best) to 4 (worst).
    It is calculated trough an algorithm using several key aspects of
    the output.
    The main entry point is the `_convert_score()` method.
    """

    def __init__(self):
        super().__init__()
        self._year = datetime.now().year
        self._score_testing = False
        self._score_mapping = {
            "lastname": "name_score",
            "phoneNumber_mobile": "mobile_score",
            "phoneNumber_number": "number_score",
            "address_current_postalCode": "address_score",
        }

    @staticmethod
    def _categorize_score(score: float) -> int:
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

    def _calc_score(self, result_tuple: Score) -> float:
        """Calculate a quality score for the found number."""
        # Set constant
        x = 100

        def date_score(result: Score, score: float) -> float:
            return (1 - ((self._year - int(result.year_of_record)) / (x / 2))) * score

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
            _x = max((self._year - result.date_of_birth) / 100, .5) if result.date_of_birth else .5
            return score if result.deceased is None else _x * score

        def n_score(result: Score, score: float) -> float:
            for var in [result.phonenumber_number, result.lastname_number]:
                score *= (1 - ((var - 1) / x))
            return score

        def fuzzy_score(result: Score, score: float) -> float:
            """Uses name_input and name_output,
            taking into account the number of changes in name."""
            n1, n2 = result.matched_names
            if n1 and n2:
                lev = levenshtein(n1, n2)
                if lev < .4 and (n1 not in n2 or n2 not in n1):
                    return 0
                return lev * score
            return score

        def missing_score(result: Score, score: float) -> float:
            if result.date_of_birth is None:
                score *= .9
            if result.gender is None:
                score *= .9
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
            return 0 if not result.matched_names[0] else score

        def persons_score(result: Score, score: float) -> float:
            return (1 - ((result.found_persons - 1) / (x / 4))) * score

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

    def _convert_score(self, result_tuple: Score) -> Union[int, float]:
        score_percentage = self._calc_score(result_tuple)
        if self._score_testing:
            return score_percentage
        categorized_score = self._categorize_score(score_percentage)
        return categorized_score


class MatchQueries:
    def __init__(self, **kwargs):
        super().__init__()
        self.data = None
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
        self._name_only_query = kwargs.pop("name_only_query", False)
        self._strictness = kwargs.pop("strictness", 5)
        self._use_sources = kwargs.pop("sources", ())

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
        yield "full", self._base_query(must=[], should=[
            {"match_phrase" if field == "lastname" else "match":
             {self._es_mapping[field]: self.data[field]}}
            for field in self.data if field != "telephone" and self.data[field]],
                                       minimum_should_match=self._strictness)
        if (self.data.postalCode
                and self.data.lastname and self.data.initials):
            yield "initial", self._base_query(must=[
                {"match": {"address.current.postalCode": self.data.postalCode}},
                {"match": {"lastname": {
                    "query": max(self.data.lastname.split(), key=len), "fuzziness": 2}}},
                {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}])
        if self.data.lastname and self.data.initials:
            if self.data.date_of_birth:
                if self.data.date_of_birth.day <= 12:
                    swapped_dob = datetime(year=self.data.date_of_birth.year,
                                           month=self.data.date_of_birth.day,
                                           day=self.data.date_of_birth.month)
                    dob = {"bool": {"minimum_should_match": 1, "should": [
                        {"match": {"birth.date": self.data.date_of_birth}},
                        {"match": {"birth.date": swapped_dob}}]}}
                else:
                    dob = {"match": {"birth.date": self.data.date_of_birth}}
                yield "dob", self._base_query(must=[
                    dob,
                    {"match": {"lastname": {
                        "query": max(self.data.lastname.split(), key=len), "fuzziness": 2}}},
                    {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}])
            if self.data.number:
                yield "number", self._base_query(must=[
                    {"match": {"phoneNumber.number": self.data.number}},
                    {"match": {"lastname": {
                        "query": max(self.data.lastname.split(), key=len), "fuzziness": 2}}},
                    {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}])
            if self.data.mobile:
                yield "mobile", self._base_query(must=[
                    {"match": {"phoneNumber.mobile": self.data.mobile}},
                    {"match": {"lastname": {
                        "query": max(self.data.lastname.split(), key=len), "fuzziness": 2}}},
                    {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}])
        if (self.data.postalCode
                and self.data.lastname):
            query = self._base_query(must=[
                {"match": {"address.current.postalCode": self.data.postalCode}},
                {"match": {"lastname": {
                    "query": max(self.data.lastname.split(), key=len), "fuzziness": 2}}}])
            if self.data.initials:
                query["query"]["bool"]["should"] = {
                    "wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}
            yield "name", query
            query = self._base_query(must=[
                {"match": {"address.current.postalCode": self.data.postalCode}},
                {"wildcard": {
                    "lastname": f"*{max(self.data.lastname.split(), key=len).lower()}*"}}])
            if self.data.initials:
                query["query"]["bool"]["should"] = {
                    "wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}
            yield "wildcard", query
        if (self.data.postalCode
                and self.data.houseNumber):
            must = [{"match": {"address.current.postalCode": self.data.postalCode}},
                    {"match": {"address.current.houseNumber": self.data.houseNumber}}]
            if self.data.houseNumberExt:
                must.append({"wildcard": {
                    "address.current.houseNumberExt":
                        f"*{self.data.houseNumberExt[0].lower()}*"}})
            yield "address", self._base_query(must=must)
        if self._name_only_query and self.data.lastname and self.data.initials:
            yield "name_only", self._base_query(
                must=[{"wildcard": {"lastname": f"*{max(self.data.lastname.split(), key=len).lower()}*"}},
                      {"wildcard": {"initials": f"{self.data.initials[0].lower()}*"}}])

    def _base_query(self, **kwargs):
        return self._extend_query({
            "query": {"bool": kwargs},
            "sort": [{"dateOfRecord": "desc"}]})

    def _extend_query(self, query):
        if self._use_sources:
            query["query"]["bool"]["must"].append({"terms": {"source": [*self._use_sources]}})
        return query

    @staticmethod
    def _id_query(responses: Sequence) -> dict:
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


class PersonData(MatchQueries,
                 SourceMatch,
                 SourceScore):
    """Match data with Matrixian's Person Database.

    Main method::
    :meth:`PersonData.match`

    The match method returns a dictionary which includes the person data,
    in addition to:
        * Several calculated scores
        * Search type
        * Match keys

    Please refer to the following page for documentation of the
    keyword arguments that are available:
    https://matrixiangroup.atlassian.net/wiki/spaces/SF/pages/1319763972/Person+matching#Tweaking-parameters

    Example::
        pm = PersonData(call_to_validate=True)
        data = {
            "initials": "P",
            "lastname": "Saalbrink",
            "postalCode": "1071XB",
            "houseNumber": "71",
            "houseNumberExt": "B",
        }
        try:
            result = pm.match(data)
            print(result)
        except NoMatch:
            pass
    :return: {
        'address_current_city': 'Amsterdam',
        'address_current_country': 'NL',
        'address_current_houseNumber': 71,
        'address_current_houseNumberExt': 'BA',
        'address_current_location': [4.88027692, 52.35333008],
        'address_current_postalCode': '1071XB',
        'address_current_state': 'Noord-Holland',
        'address_current_street': 'Ruysdaelstraat',
        'address_score': 'C2',
        'date': datetime.datetime(2018, 12, 20, 0, 0),
        'gender': 'M',
        'initials': 'PP',
        'lastname': 'Saalbrink',
        'match_keys': {'lastname', 'initials', 'address'},
        'mobile_score': 'C1',
        'name_score': 'C2',
        'number_score': 'C2',
        'phoneNumber_country': '+31',
        'phoneNumber_mobile': 649978891,
        'phoneNumber_number': 203345554,
        'search_type': 'initial',
        'source': 'company_data_NL_contact'
    }
    """
    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        # data holders
        self.result = self.data = None
        self._clean = Cleaner().clean

        # connectors
        self._es = ESClient(PD_INDEX)
        self._vn = ESClient(VN_INDEX)
        if gethostname() == "matrixian":
            self._phone_url = "http://localhost:5000/call/"
        else:
            self._phone_url = "http://94.168.87.210:4000/call/"
        self._email_url = ("http://develop.platform.matrixiangroup.com"
                           ":4000/email?email=")

        # kwargs
        self._email = kwargs.pop("email", False)
        self._use_id_query = kwargs.pop("id_query", False)
        self._respect_hours = kwargs.pop("respect_hours", True)
        self._score_testing = kwargs.pop("score_testing", False)
        self._call_to_validate = kwargs.pop("call_to_validate", False)
        self._response_type = kwargs.pop("response_type", "all")
        categories = ("all", "name", "address", "phone")
        if (self._response_type not in categories and
                not isinstance(self._response_type, (tuple, list))):
            raise MatchError(f"Requested fields should be one"
                             f" of {', '.join(categories)}")
        self._countries = {"nederland", "netherlands", "nl", "nld"}

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
                "common",
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
                "common",
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
                "common",
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
        if isinstance(self._response_type, (tuple, list)):
            return tuple(f for f in self._response_type
                         if f != "phoneNumber_country")
        elif self._response_type == "phone":
            return "phoneNumber_number", "phoneNumber_mobile"
        elif self._response_type == "address":
            return "address_current_postalCode",
        elif self._response_type == "name":
            return "lastname",
        else:
            return ("lastname", "address_current_postalCode",
                    "phoneNumber_number", "phoneNumber_mobile")

    def _check_country(self, country: str):
        if country and country.lower() not in self._countries:
            raise NoMatch(f"Not implemented for country {country}.")

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
                if self._responses[key]["_id"] != response["_id"]:
                    occurring = True
        return occurring

    def _find(self):
        debug("Data = %s", self.data)
        self.result = {}
        self._responses = {}
        for _type, q in self._queries:
            if self._use_id_query:
                responses = [{"_id": d["_id"], **d["_source"]}  # noqa
                             for d in self._es.find(
                        self._id_query(self._es.find(
                            q, source_only=True)))]
            else:
                responses = [{"_id": d["_id"], **d["_source"]}  # noqa
                             for d in self._es.find(q)]
            for response in responses:
                response = flatten(response)
                for key in self._requested_fields:
                    if key not in self.result and response.get(key):
                        if (key in ("phoneNumber_number", "phoneNumber_mobile")
                                and not self._phone_valid(response[key])):
                            continue
                        if (key in ("address_moved", "birth_date", "death_date")
                                and response[key] == DEFAULT_DATE):
                            continue
                        if (key == "contact_email" and
                                not self._email_valid(response[key])):
                            continue
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
        valid = is_valid_number(phoneparse(phone, "NL"))
        if f"{number}".startswith(("8", "9")):
            valid = False
        if valid:
            with suppress(ElasticsearchException):
                query = {"query": {"bool": {"must": [{"match": {"phoneNumber": number}}]}}}
                result = self._vn.find(query=query, first_only=True)
                if result:
                    return result["valid"]
            if self._call_to_validate:
                if self._respect_hours:
                    t = localtime().tm_hour
                    while t >= 22 or t < 8:
                        sleep(60)
                        t = localtime().tm_hour
                while True:
                    with suppress(RetryError, MaxRetryError):
                        response = get(f"{self._phone_url}{phone}",
                                       auth=("datateam", "matrixian"))
                        if response.ok:
                            valid = loads(response.text)
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
                    year_of_record=response["dateOfRecord"][:4],
                    deceased=response["death_year"],
                    lastname_number=len(set(d["lastname"] for d in self._responses.values())),
                    gender=response["gender"],
                    date_of_birth=response["birth_year"],
                    phonenumber_number=len(set(d[key] for d in self._responses.values()))
                    if "phoneNumber" in key else 1,
                    occurring=self._check_match(key),
                    moved=response["address_moved"] != DEFAULT_DATE,
                    mobile="mobile" in key or "lastname" in key,
                    matched_names=(self.data.lastname, response["lastname"]),
                    found_persons=len({response["id"] for response in self._responses.values()}),
                ))
                self.result[self._score_mapping.get(key, f"{key}_score")] = f"{source}{score}"

    def _finalize(self):
        # Get match keys
        self._set_match(self.result)
        self.result["match_keys"].update(self._match_keys)

        # Fix dates
        for key in ("date", "address_moved", "birth_date", "death_date"):
            if key in self.result and isinstance(self.result[key], str):
                self.result[key] = dateparse(self.result[key], ignoretz=True)

        debug("Result = %s", self.result)

    def match(self, data: dict) -> dict:
        self._check_country(data.pop("country", "nl"))
        self.data = self._clean(Data(**data))
        self._find()
        if self.result:
            self._get_score()
            self._finalize()
            return self.result
        else:
            raise NoMatch


class NamesData:
    def __init__(self):
        self.es = ESClient("dev_peter.names_data")
        self.uncommon_initials = {
            "I",
            "K",
            "N",
            "O",
            "Q",
            "U",
            "V",
            "X",
            "Y",
            "Z",
        }
        self.initial_freq = {
            "A": 0.10395171481742668,
            "B": 0.02646425590465198,
            "C": 0.0624646058908782,
            "D": 0.02830463793241263,
            "E": 0.04368881360131388,
            "F": 0.028797360001816555,
            "G": 0.046652085281351154,
            "H": 0.06267525720019877,
            "I": 0.013224965395575616,
            "J": 0.1499112822017472,
            "K": 0.015244050637799001,
            "L": 0.03944411414236686,
            "M": 0.14917774707325376,
            "N": 0.01947222640467436,
            "O": 0.003400582698897405,
            "P": 0.04273322203716372,
            "Q": 0.0010834182963716224,
            "R": 0.040866938005614986,
            "S": 0.03767597603233326,
            "T": 0.03086106951847034,
            "U": 0.0004628351419562074,
            "V": 0.005300463759232,
            "W": 0.040004889089096926,
            "X": 0.0005470681834522827,
            "Y": 0.005756428343154263,
            "Z": 0.0017861046192925779
        }

    def affixes(self) -> set:
        return {doc["_source"]["affix"] for doc in self.es.findall(
            {"query": {"bool": {"must": {"match": {"data": "affixes"}}}}}
        )}

    def first_names(self) -> dict:
        """Import a file with first names and gender occurrence, and return a {first_name: gender} dictionary.

        This function returns if any given Dutch first name has more male or female bearers. If the number is equal,
        None is returned. Names are cleaned before output.

        The output can be used to fill missing gender data."""
        return {doc["_source"]["firstname"]: doc["_source"]["gender"] for doc in  # noqa
                self.es.findall(
                    {"query": {"bool": {"must": {"match": {"data": "firstnames"}}}}})}

    def titles(self) -> set:
        """Imports a file with titles and returns them as a set. The output can be used to clean last name data."""
        return set(doc["_source"]["title"] for doc in  # noqa
                   self.es.findall(
                       {"query": {"bool": {"must": {"match": {"data": "titles"}}}}}))

    def surnames(self) -> dict:
        """Imports a database with surnames frequencies and returns common surnames as a list.

        Only surnames that occur more than 200 times in the Netherlands are regarded as common.
        If a name occurs twice in the file, the largest number is taken.

        The output can be used for data and matching quality calculations."""

        names_data = self.es.findall({"query": {"bool": {"must": {"match": {"data": "surnames"}}}}})
        # Return only names that occur commonly
        names_data = {doc["_source"]["surname"]: doc["_source"]["number"]  # noqa
                      for doc in names_data}
        return names_data


_module_data = {}


class Cleaner:

    def __init__(self):
        self.data = {}
        if "title_data" not in _module_data:
            _module_data["title_data"] = NamesData().titles()

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
                    number_valid = is_valid_number(phoneparse(f"+31{self.data[_type]}", "NL"))
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
            if self.data["lastname"] and self.data["lastname"].split()[-1].lower() in _module_data["title_data"]:
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
