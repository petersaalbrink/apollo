"""Module for querying Matrixian's person database.

This module contains classes that accept some input and then return any
matching records from Matrixian's person database. The main entry point
for this is the `match()` method of the `PersonData()` class. This is
also the main method used by the `PersonChecker` of the CDQC product
(see the `data_team_validators` repository).

Additionally, there is a `NamesData` class which provides some methods
for easy access to data on several names statistics, such as last name
occurrence in the Netherlands; and a `MatchMetrics` class which can be
used to collect statistics on a document from the `person_data`
collection.

Other useful objects include the `Data` class, which can serve as a
container for person data, and the `Cleaner` class, which cleans said
data.

.. py:class:: common.persondata.Cleaner
   Clean input data for use with PersonData.

.. py:class:: common.persondata.Data
   Dataclass for person input.

.. py:class:: common.persondata.MatchMetrics
   Access data on person statistics.

.. py:class:: common.persondata.NamesData
   Access data on several names statistics.

.. py:class:: common.persondata.PersonData
   Match data with Matrixian's Person Database.
"""

from collections.abc import MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache, partial
from itertools import combinations
from logging import debug
from math import prod
from re import sub
from typing import Iterator, Optional, Sequence, Tuple, Union

from dateutil.parser import parse as dateparse
from text_unidecode import unidecode

import common.api.phone
from .api.email import check_email
from .connectors.mx_elastic import ESClient
from .connectors.mx_mongo import MongoDB
from .exceptions import MatchError, NoMatch
from .parsers import flatten, levenshtein

ND_INDEX = "cdqc.names_data"
PD_INDEX = "cdqc.person_data"
HOST = "cdqc"

DATE_FORMAT = "%Y-%m-%d"
DEFAULT_DATE = "1900-01-01"

ADDRESS_KEY = "address"
DATE_KEYS = {"address_moved", "birth_date", "death_date"}
EMAIL_KEY = "contact_email"
PERSONAL_KEYS = ("details", "birth")
PHONE_KEYS = {"phoneNumber_number", "phoneNumber_mobile"}

_module_data = {}
_extra_fields = {
    "birth.date": 1 / 38,
    "phoneNumber.mobile": 1 / 38,
    "phoneNumber.number": 1 / 26,
    "address.address_id": 1 / 26,
}
_extra_fields = {
    field: _extra_fields[field]
    for field in (
        "birth.date",
        "phoneNumber.mobile",
        "phoneNumber.number",
        "address.address_id",
    )
}


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
    firstname: str = None
    date_of_birth: datetime = None
# TODO: convert to pydantic.BaseModel


@dataclass(frozen=True)
class _Score:
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


class _SourceMatch:
    """Calculates the "certainty" part of the output score.

    This score will be a letter ranging from A (best) to D (worst).
    It is calculated by comparing several keys of the output to the
    input.
    The main entry point is the `_get_source()` method.
    """
    data: Optional[Data] = None
    _source: Optional[str] = None
    _matched = {}
    _source_match = {6: "A", 5: "A", 4: "A", 3: "B", 2: "C", 1: "D"}

    def _lastname_match(self, response):
        """Does the last name match?"""
        return (response.get("details_lastname") and self.data.lastname
                and (response["details_lastname"] in self.data.lastname
                     or self.data.lastname in response["details_lastname"]
                     or set(self.data.lastname.split()).issubset(set(response["details_lastname"].split()))
                     or set(response["details_lastname"].split()).issubset(set(self.data.lastname.split()))
                     or levenshtein(response.get("details_lastname", ""),
                                    self.data.lastname,
                                    measure="distance") < 2)
                ) or False

    def _initials_match(self, response):
        """Do the initials match?"""
        try:
            return (response.get("details_initials") and self.data.initials
                    and response["details_initials"][0] == self.data.initials[0]
                    ) or False
        except IndexError:
            return False

    def _gender_match(self, response):
        """Does the gender match?"""
        return (response.get("details_gender") and self.data.gender
                and response["details_gender"] == self.data.gender
                ) or False

    def _address_match(self, response):
        """Do the postal code and house number match?"""
        return (response.get("address_postalCode")
                and self.data.postalCode
                and response.get("address_houseNumber")
                and self.data.houseNumber
                and response["address_postalCode"] == self.data.postalCode
                and response["address_houseNumber"] == self.data.houseNumber
                ) or False

    def _phone_match(self, response):
        """Do the phone numbers match?"""
        return ((response.get("phoneNumber_mobile")
                 and self.data.mobile
                 and response["phoneNumber_mobile"] == self.data.mobile
                 ) or (response.get("phoneNumber_number")
                       and self.data.number and
                       response["phoneNumber_number"] == self.data.number)
                ) or False

    def _dob_match(self, response):
        """Does the date of birth match?"""
        return (response["birth_date"][:10] != DEFAULT_DATE
                and self.data.date_of_birth
                and response["birth_date"][:10] == self.data.date_of_birth.strftime(DATE_FORMAT)
                ) or False

    def _set_match(self, response: dict):
        """Take a person matching response, and store the match properties."""
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
        """Get the matching keys from the match properties."""
        return {key for key in self._matched if self._matched[key]}

    @property
    def _match_source(self) -> str:
        """Decide which fields are the source of the match."""
        return self._source_match[sum(map(bool, self._matched.values()))]

    def _get_source(self, response: dict):
        """Get the source of the person match."""
        self._set_match(response)
        try:
            self._source = self._match_source
        except KeyError as e:
            raise MatchError(
                "No source could be defined for this match!",
                self.data, response) from e


class _SourceScore:
    """Calculates the "quality" part of the output score.

    This score will be a number ranging from 1 (best) to 4 (worst).
    It is calculated trough an algorithm using several key aspects of
    the output.
    The main entry point is the `_calc_score()` method.
    """
    _year = datetime.now().year
    _score: Optional[int] = None
    _score_mapping = {
        "details_lastname": "name_score",
        "birth_date": "dob_score",
        "phoneNumber_mobile": "mobile_score",
        "phoneNumber_number": "number_score",
        "address_postalCode": "address_score",
    }
    _source_valuation = {
        "saneringen": 1,
        "Kadaster_4": 1,
        "Kadaster_3": 1,
        "Kadaster_2": 1,
        "Kadaster_1": 1,
        "company_data_NL_contact": 1,
        "shop_data_nl_main": 1,

        "whitepages_nl_2020_final": 2,
        "whitepages_nl_2019": 2,
        "whitepages_nl_2018": 2,

        "YP_CDs_Consumer_main": 3,
        "car_data_names_only_dob": 3,
        "yp_2004_2005": 3,

        "postregister_dm": 4,
        "Insolventies_main": 4,
        "GevondenCC": 4,
        "consumer_table_distinct_indexed": 4,
    }

    @lru_cache()
    def _calc_score(self, result_tuple: _Score) -> int:
        """Calculates and categorizes a match quality score
        based on match properties.
        """

        def data_score(result: _Score):
            """Score is lowest if there was no lastname input."""
            if not result.matched_names[0]:
                self._score = 4

        def occurring_score(result: _Score):
            """Score is zero if a number occurs more recently elsewhere."""
            if result.occurring:
                self._score = 4

        def death_score(result: _Score):
            """Lowest score if a person is deceased."""
            if result.deceased:
                self._score = 4

        def date_score(result: _Score):
            """The older the record, the lower the score."""
            delta = self._year - int(result.year_of_record)
            if delta <= 4:
                self._score = 1
            elif delta <= 8:
                self._score = 2
            elif delta <= 12:
                self._score = 3
            else:
                self._score = 4

        def source_score(result: _Score):
            """Lower quality of the record's source means a lower score."""
            self._score = max((self._score, self._source_valuation[result.source]))

        def moved_score(result: _Score):
            """Lower score if there was an address movement."""
            if result.moved:
                if result.mobile:
                    self._score = max((self._score, 2))
                else:
                    self._score = 4

        def missing_score(result: _Score):
            """Lower score if date of birth or gender is missing."""
            if result.date_of_birth is None:
                self._score = max((self._score, 2))
            if result.gender is None:
                self._score = max((self._score, 2))

        def fuzzy_score(result: _Score):
            """Uses name_input and name_output,
            taking into account the number of changes in name."""
            if all(result.matched_names):
                lev = levenshtein(*result.matched_names)
                if lev >= .8:
                    self._score = max((self._score, 1))
                elif lev >= .6:
                    self._score = max((self._score, 2))
                elif lev >= .4:
                    self._score = max((self._score, 3))
                else:
                    self._score = 4
            else:
                self._score = 4

        def n_score(result: _Score):
            """The more different names and numbers, the lower the score."""
            self._score = max((
                self._score,
                min((result.phonenumber_number, 4)),
                min((result.lastname_number, 4)),
                min((result.found_persons, 4)),
            ))

        def full_score(result: _Score):
            """Calculate a score from all the scoring properties."""
            if result is None:
                raise NoMatch
            self._score = 1
            for func in (
                    data_score,
                    occurring_score,
                    death_score,
                    date_score,
                    source_score,
                    moved_score,
                    missing_score,
                    fuzzy_score,
                    n_score,
            ):
                func(result)
                if self._score == 4:
                    break

        return full_score(result_tuple)


class _MatchQueries:
    """Provide Elasticsearch queries for person matching.

    This class provides a property, `_queries`, that holds an iterator
    which contains several queries in order of decreasing quality or
    strictness. These are returned as name-query pairs.
    """
    data: Optional[Data] = None
    _es_mapping = {
        "lastname": "details.lastname",
        "initials": "details.initials",
        "postalCode": "address.postalCode",
        "houseNumber": "address.houseNumber",
        "houseNumberExt": "address.houseNumberExt",
        "mobile": "phoneNumber.mobile",
        "number": "phoneNumber.number",
        "gender": "details.gender",
        "firstname": "details.firstname",
        "date_of_birth": "birth.date",
    }

    def __init__(self, **kwargs):
        self._address_query = kwargs.pop("address_query", False)
        self._name_only_query = kwargs.pop("name_only_query", False)
        self._strictness = kwargs.pop("strictness", 5)
        self._use_sources = kwargs.pop("sources", ())

    @property
    def _queries(self) -> Iterator[Tuple[str, dict]]:
        """Iterator containing queries in order of decreasing quality.

        Returns the following queries:
        dob: query with must-clause (lastname, initials, date_of_birth)
        full: query with should-clause containing all possible fields
        initial: query with must-clause (lastname, initials, address)
        number: query with must-clause (lastname, initials, phone number)
        mobile: query with must-clause (lastname, initials, mobile number)
        name: query with must-clause (lastname, initials, postalCode)
        wildcard: query with must-clause (lastname, initials, postalCode)
        address: query with must-clause (address)
        name_only: query with must-clause (lastname and initials)
        """
        def lastname_clause(f: int = 1):
            if "ij" in self.data.lastname:
                clause = {"bool": {"should": [
                    {"match": {"details.lastname": {"query": self.data.lastname.replace("ij", "y"), "fuzziness": f}}},
                    {"match": {"details.lastname": {"query": self.data.lastname, "fuzziness": f}}},
                ], "minimum_should_match": 1}}
            elif "y" in self.data.lastname:
                clause = {"bool": {"should": [
                    {"match": {"details.lastname": {"query": self.data.lastname.replace("y", "ij"), "fuzziness": f}}},
                    {"match": {"details.lastname": {"query": self.data.lastname, "fuzziness": f}}},
                ], "minimum_should_match": 1}}
            else:
                clause = {"match": {"details.lastname": {"query": self.data.lastname, "fuzziness": f}}}
            return clause

        if self.data.lastname and self.data.initials and self.data.date_of_birth:
            if self.data.date_of_birth.day <= 12:
                swapped_dob = datetime(year=self.data.date_of_birth.year,
                                       month=self.data.date_of_birth.day,
                                       day=self.data.date_of_birth.month)
                dob = {"bool": {"minimum_should_match": 1, "should": [
                    {"term": {"birth.date": self.data.date_of_birth}},
                    {"term": {"birth.date": swapped_dob}}]}}
            else:
                dob = {"term": {"birth.date": self.data.date_of_birth}}
            yield "dob", self._base_query(must=[
                dob, lastname_clause(),
                {"bool": {"should": [
                    {"wildcard": {"details.initials": self.data.initials[0]}},
                    {"wildcard": {"details.initials": {"value": self.data.initials[1], "boost": 2}}},
                ], "minimum_should_match": 1}},
            ], person=True)

        yield "full", self._base_query(
            must=[], should=[{"wildcard" if field == "initials" else "match": {
                self._es_mapping[field]: {"query": self.data[field], "boost": 2}
                if field == "lastname" else (
                    {"query": self.data[field], "boost": 4} if field == "date_of_birth" else (
                        self.data[field][1] if field == "initials" else self.data[field]))}}
                for field in self.data if field != "telephone" and self.data[field]],
            minimum_should_match=self._strictness,
            person=True)

        if self.data.lastname:
            lastname = lastname_clause()

            if self.data.initials:

                if self.data.postalCode:
                    yield "initial", self._base_query(must=[
                        {"term": {"address.postalCode.keyword": self.data.postalCode}},
                        lastname,
                        {"wildcard": {"details.initials": self.data.initials[0]}}],
                        person=True)

                if self.data.number:
                    yield "number", self._base_query(must=[
                        {"term": {"phoneNumber.number": self.data.number}},
                        lastname,
                    ], should=[{"wildcard": {"details.initials": self.data.initials[0]}}])

                if self.data.mobile:
                    yield "mobile", self._base_query(must=[
                        {"term": {"phoneNumber.mobile": self.data.mobile}},
                        lastname,
                    ], should=[{"wildcard": {"details.initials": self.data.initials[0]}}])

            if self.data.postalCode:

                query = self._base_query(must=[
                    {"term": {"address.postalCode.keyword": self.data.postalCode}},
                    lastname])
                if self.data.initials:
                    query["query"]["bool"]["should"] = {
                        "wildcard": {"details.initials": self.data.initials[0]}}
                yield "name", query

                query = self._base_query(must=[
                    {"term": {"address.postalCode.keyword": self.data.postalCode}},
                    {"wildcard": {"details.lastname": f"*{self.data.lastname.lower()}*"}}])
                if self.data.initials:
                    query["query"]["bool"]["should"] = {
                        "wildcard": {"details.initials": self.data.initials[0]}}
                yield "wildcard", query

        if self._address_query and self.data.postalCode and self.data.houseNumber:
            must = [{"term": {"address.postalCode.keyword": self.data.postalCode}},
                    {"term": {"address.houseNumber": self.data.houseNumber}}]
            if self.data.houseNumberExt:
                must.append({"wildcard": {
                    "address.houseNumberExt": f"*{self.data.houseNumberExt[0].lower()}*"}})
            yield "address", self._base_query(must=must)

        if self._name_only_query and self.data.lastname and self.data.initials:
            yield "name_only", self._base_query(
                must=[{"wildcard": {"details.lastname": f"*{self.data.lastname.lower()}*"}},
                      {"wildcard": {"details.initials": self.data.initials[0]}}])

    def _base_query(self, person: bool = False, **kwargs):
        """Encapsulate clauses in a bool query, with sorting on date."""
        return self._extend_query(query={
            "query": {"bool": kwargs},
            "sort": [{"_score": "desc"}, {"date": "desc"}]},
            person=person)

    def _extend_query(self, query: dict, person: bool):
        """Extend a complete query with some restrictions."""
        if person and self.data.date_of_birth:
            clause = {"bool": {"should": [
                {"term": {"birth.date": self.data.date_of_birth}},
                {"term": {"birth.date": DEFAULT_DATE}},
            ],
                "minimum_should_match": 1,
            }}
            try:
                query["query"]["bool"]["must"].append(clause)
            except KeyError:
                query["query"]["bool"]["must"] = clause
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
                        {"term": {"id": _id}}
                        for _id in ordered_set(responses)],
                    "minimum_should_match": 1
                }
            },
            "sort": {"date": "desc"},
        }


class NamesData:
    """Access data on several names statistics.

    These include:
        * initials with frequencies
        * possible affixes (prefixes and suffixes)
        * first names and accompanying genders
        * possible titles
        * last names and their occurrences

    All data pertains to the Netherlands, and is loaded using Elasticsearch.
    """
    def __init__(self):
        self.es = ESClient(ND_INDEX, host=HOST)
        self.es.index_exists = True
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
        """Load a set with affixes.

        The output can be used to clean last name data.
        """
        return {doc["_source"]["affix"] for doc in self.es.findall(
            {"query": {"bool": {"must": {"term": {"data": "affixes"}}}}}
        )}

    def first_names(self) -> dict:
        """Load a dictionary with first names and gender occurrence.

        This function returns if any given Dutch first name has more
        male or female bearers. If the number is equal, None is
        returned. Names are cleaned before output.

        The output can be used to fill missing gender data.
        """
        return {doc["_source"]["firstname"]: doc["_source"]["gender"] for doc in  # noqa
                self.es.findall(
                    {"query": {"bool": {"must": {"term": {"data": "firstnames"}}}}})}

    def titles(self) -> set:
        """Load a set with titles.

        The output can be used to clean last name data.
        """
        return set(doc["_source"]["title"] for doc in  # noqa
                   self.es.findall(
                       {"query": {"bool": {"must": {"term": {"data": "titles"}}}}}))

    def surnames(self) -> dict:
        """Load a dictionary with surnames and their numbers.

        The output can be used for data and matching quality calculations.
        """
        return {doc["_source"]["surname"]: doc["_source"]["number"]  # noqa
                for doc in self.es.findall(
                {"query": {"bool": {"must": {"term": {"data": "surnames"}}}}})}


class Cleaner:
    """Clean input data for use with PersonData.

    The main entry point is the `.clean()` method.
    """

    def __init__(self):
        """Make a cleaner.

        Loads the NamesData.titles into the module if they're haven't
        been loaded already.
        """
        self.data = {}
        self._affixes = (" Van ", " Het ", " De ")
        self._phone_fields = ("number", "telephone", "mobile")

    def clean(self, data: Union[Data, dict]) -> Union[Data, dict]:
        self.data = data
        for function in [function for function in dir(self)
                         if function.startswith("_clean")]:
            with suppress(KeyError):
                self.__getattribute__(function)()
        return self.data

    def _clean_phones(self):
        """Clean and parse phone and mobile numbers."""
        for _type in self._phone_fields:
            if self.data.get(_type):
                try:
                    parsed = common.api.phone.parse_phone(self.data[_type], "NL")
                    if parsed.is_valid_number:
                        self.data[_type] = parsed.national_number
                        if _type == "telephone":
                            if f"{self.data[_type]}".startswith("6"):
                                self.data["mobile"] = self.data.pop(_type)
                            else:
                                self.data["number"] = self.data.pop(_type)
                    else:
                        self.data.pop(_type)
                except common.api.phone.PhoneApiError:
                    self.data.pop(_type)

    def _clean_initials(self):
        """Clean initials."""
        if isinstance(self.data["initials"], str):
            self.data["initials"] = sub(r"[^A-Za-z\u00C0-\u017F]", "",
                                        self.data["initials"].upper())
        if not self.data["initials"]:
            self.data.pop("initials")

    def _clean_gender(self):
        """Clean gender."""
        if isinstance(self.data["gender"], str) and self.data["gender"] in (
                "Man", "Vrouw", "MAN", "VROUW", "man", "vrouw", "m", "v", "M", "V"):
            self.data["gender"] = self.data["gender"].upper().replace("MAN", "M").replace("VROUW", "V")
        else:
            self.data.pop("gender")

    def _clean_lastname(self):
        """Clean last name.

        Keep only letters; hyphens become spaces.
        Remove all special characters and titles.
        """
        if isinstance(self.data["lastname"], str):
            self.data["lastname"] = self.data["lastname"].title()
            for o in self._affixes:
                self.data["lastname"] = self.data["lastname"].replace(o, " ")
            self.data["lastname"] = sub(r"-", " ", self.data["lastname"])
            self.data["lastname"] = sub(r"[^\sA-Za-z\u00C0-\u017F]", "", self.data["lastname"])
            self.data["lastname"] = unidecode(self.data["lastname"].strip())
            try:
                if self.data["lastname"] and self.data["lastname"].split()[-1].lower() in _module_data["title_data"]:
                    self.data["lastname"] = " ".join(self.data["lastname"].split()[:-1])
            except KeyError:
                _module_data["title_data"] = NamesData().titles()
                if self.data["lastname"] and self.data["lastname"].split()[-1].lower() in _module_data["title_data"]:
                    self.data["lastname"] = " ".join(self.data["lastname"].split()[:-1])
        if not self.data["lastname"]:
            self.data.pop("lastname")

    def _clean_dob(self):
        """Clean and parse date of birth."""
        if self.data["date_of_birth"] and isinstance(self.data["date_of_birth"], str):
            self.data["date_of_birth"] = self.data["date_of_birth"].split()[0]
            try:
                self.data["date_of_birth"] = datetime.strptime(self.data["date_of_birth"][:10], DATE_FORMAT)
            except ValueError:
                try:
                    self.data["date_of_birth"] = dateparse(self.data["date_of_birth"], ignoretz=True)
                except ValueError:
                    self.data["date_of_birth"] = None
        if not self.data["date_of_birth"]:
            self.data.pop("date_of_birth")

    def _clean_pc(self):
        """Clean postal code."""
        if isinstance(self.data["postalCode"], str):
            self.data["postalCode"] = self.data["postalCode"].replace(" ", "").upper()
            if len(self.data["postcode"]) != 6:
                self.data.pop("postalCode")
        else:
            self.data.pop("postalCode")

    def _clean_hn(self):
        """Clean house number."""
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
        """Clean house number extension."""
        if isinstance(self.data["houseNumberExt"], str):
            self.data["houseNumberExt"] = sub(r"[^A-Za-z0-9\u00C0-\u017F]", "",
                                              self.data["houseNumberExt"].upper())
            self.data["houseNumberExt"] = sub(r"\D+(?=\d)", "", self.data["houseNumberExt"])
        if not self.data["houseNumberExt"]:
            self.data.pop("houseNumberExt")


class PersonData(_MatchQueries,
                 _SourceMatch,
                 _SourceScore):
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
    """

    _categories = ("all", "name", "address", "phone")
    _countries = {"nederland", "netherlands", "nl", "nld"}

    def __init__(self, **kwargs):
        """Make an object for person matching (not threadsafe)."""

        super().__init__(**kwargs)
        self.result: Optional[dict] = None
        self.data: Optional[Data] = None

        # connectors
        self._es = ESClient(PD_INDEX, host=HOST)
        self._es.index_exists = True
        self._es_find = partial(self._es.find, with_id=True)

        common.api.phone.RESPECT_HOURS = kwargs.pop("respect_hours", True)
        common.api.phone.CALL_TO_VALIDATE = kwargs.pop("call_to_validate", False)
        self._check_phone = partial(
            common.api.phone.check_phone,
            valid=True,
            call=common.api.phone.CALL_TO_VALIDATE,
        )

        # kwargs
        self._email = kwargs.pop("email", False)
        self._use_id_query = kwargs.pop("id_query", False)
        self._response_type = kwargs.pop("response_type", "all")
        if (self._response_type not in self._categories and
                not isinstance(self._response_type, (tuple, list))):
            raise MatchError(f"Requested fields should be one"
                             f" of {', '.join(self._categories)}")

    def __repr__(self):
        return f"PersonData(in={self.data}, out={self.result})"

    @property
    def _requested_fields(self) -> tuple:
        """Fields to return, based on response_type."""
        if isinstance(self._response_type, (tuple, list)):
            return self._response_type
        elif self._response_type == "all":
            return (
                "address_city",
                "address_country",
                "address_houseNumber",
                "address_houseNumberExt",
                "address_location",
                "address_postalCode",
                "address_state",
                "address_street",
                "address_moved",
                "birth_date",
                "contact_email",
                "death_date",
                "details_firstname",
                "details_gender",
                "details_initials",
                "details_lastname",
                "details_middlename",
                "phoneNumber_country",
                "phoneNumber_mobile",
                "phoneNumber_number",
            ) if self._email else (
                "address_city",
                "address_country",
                "address_houseNumber",
                "address_houseNumberExt",
                "address_location",
                "address_postalCode",
                "address_state",
                "address_street",
                "address_moved",
                "birth_date",
                "death_date",
                "details_firstname",
                "details_gender",
                "details_initials",
                "details_lastname",
                "details_middlename",
                "phoneNumber_country",
                "phoneNumber_mobile",
                "phoneNumber_number",
            )
        elif self._response_type == "name":
            return (
                "birth_date",
                "death_date",
                "details_firstname",
                "details_gender",
                "details_initials",
                "details_lastname",
                "details_middlename",
            )
        elif self._response_type == "address":
            return (
                "address_city",
                "address_country",
                "address_houseNumber",
                "address_houseNumberExt",
                "address_location",
                "address_postalCode",
                "address_state",
                "address_street",
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
        """Fields to score, based on response_type."""
        if isinstance(self._response_type, (tuple, list)):
            return tuple(f for f in self._response_type
                         if f != "phoneNumber_country")
        elif self._response_type == "phone":
            return "phoneNumber_number", "phoneNumber_mobile"
        elif self._response_type == "address":
            return "address_postalCode",
        elif self._response_type == "name":
            return "details_lastname",
        else:
            return ("details_lastname", "birth_date", "address_postalCode",
                    "phoneNumber_number", "phoneNumber_mobile")

    def _check_country(self, country: str):
        """Check if input country is accepted."""
        if country and country.lower() not in self._countries:
            raise NoMatch(f"Not implemented for country {country}.")

    def _check_match(self, key: str):
        """Matches where we found a phone number, but the phone number
        occurs more recently on another address, or with another
        lastname, should get a lower score."""
        if "phoneNumber" in key:
            response = self._es.find({
                "query": {
                    "bool": {
                        "must":
                            {"match": {key.replace("_", "."): self.result[key]}}
                    }},
                "sort": {"date": "desc"}
            }, size=1)
            if response and self._responses[key]["_id"] != response["_id"]:
                return True
        return False

    def _find(self):
        """Main logic for finding a match.

        Iterates over the queries until a satisfying result is found.
        """
        debug("Data = %s", self.data)
        self.result = {}
        self._responses = {}

        for _type, q in self._queries:

            if self._use_id_query:
                responses = self._es_find(self._id_query(self._es.find(q)))
            else:
                responses = self._es_find(q)

            for response in responses:

                response = flatten(response)
                for key in self._requested_fields:
                    if key not in self.result and response.get(key):
                        skip_key = (
                                (key in PHONE_KEYS
                                 and not self._check_phone(response[key]))
                                or (key in DATE_KEYS
                                    and response[key][:10] == DEFAULT_DATE)
                                or (_type == ADDRESS_KEY
                                    and key.startswith(PERSONAL_KEYS))
                                or (key == EMAIL_KEY
                                    and not check_email(response[key]))
                        )
                        if skip_key:
                            continue
                        self.result[key] = response[key]
                        if key in self._main_fields:
                            self._responses[key] = response
                        self.result["search_type"] = _type
                        self.result["source"] = response["source"]
                        self.result["date"] = response["date"]

                if all(map(self.result.get, self._main_fields)):
                    return

    def _wrap_find(self):
        if self.data.initials:
            initials, self.data.initials = self.data.initials, (
                f"{self.data.initials[0].lower()}*", f"{self.data.initials.lower()}*")
            self._find()
            self.data.initials = initials
        else:
            self._find()

    def _get_score(self):
        """After a result has been found, calculate the score for this match."""
        self.result["match_keys"] = set()
        if not [key for key in self._main_fields if key in self._responses]:
            raise NoMatch
        for key in self._main_fields:
            if key in self._responses:
                response = self._responses[key]
                try:
                    self._get_source(response)
                except MatchError:
                    continue
                self.result["match_keys"].update(self._match_keys)
                self._calc_score(_Score(
                    source=response["source"],
                    year_of_record=response["date"][:4],
                    deceased=response["death_year"],
                    lastname_number=len(set(d["details_lastname"] for d in self._responses.values())),
                    gender=response["details_gender"],
                    date_of_birth=response["birth_year"],
                    phonenumber_number=len(set(d[key] for d in self._responses.values()))
                    if "phoneNumber" in key else 1,
                    occurring=self._check_match(key),
                    moved=response["address_moved"][:10] > response["date"][:10],
                    mobile="number" not in key,
                    matched_names=(self.data.lastname, response["details_lastname"]),
                    found_persons=len({response["id"] for response in self._responses.values()}),
                ))
                self.result[self._score_mapping.get(key, f"{key}_score")] = f"{self._source}{self._score}"

    def _finalize(self):
        """After getting and scoring the result, complete the output."""
        # Get match keys
        self.result["match_keys"].update(self._match_keys)

        # Fix dates
        for key in ("date", "address_moved", "birth_date", "death_date"):
            if key in self.result and isinstance(self.result[key], str):
                self.result[key] = datetime.strptime(self.result[key][:10], DATE_FORMAT)

        debug("Result = %s", self.result)

    def match(self, data: dict) -> dict:
        """Match input data to the person database.

        :raises: NoMatch
        """

        self._check_country(data.pop("country", "nl"))
        self.data = Cleaner().clean(Data(**data))

        self._wrap_find()

        if self.result:
            self._get_score()
            self._finalize()
            return self.result
        else:
            raise NoMatch


class MatchMetrics:
    """Get match metrics for a person data record.

    Main method::
        :meth:`MatchMetrics.get_metrics`

    Main attributes::
        :attr:`MatchMetrics.counts`
        :attr:`MatchMetrics.response`

    Example::
        from common.connectors import ESClient
        from common.persondata import MatchMetrics
        es = ESClient(MatchMetrics.collection)
        mm_doc = es.find({"query": {"match_all": {}}}, size=1, with_id=True)
        match_metrics = MatchMetrics(mm_doc)
        match_metrics.get_metrics()
        print(match_metrics.counts)
        print(match_metrics.response)
    """
    alpha = .05
    collection = "cdqc.person_data"
    combinations = {
        t: prod(map(_extra_fields.get, t))
        for t in [
            t for r in range(4, 0, -1) for t in
            combinations(_extra_fields, r=r)
        ]
    }

    def __init__(self, doc):
        self.doc = doc
        self.counts = {}
        self.response = {}
        self.esl = ESClient(f"{self.collection}_lastname_occurrence")
        self.esl.index_exists = True
        try:
            self.initials = _module_data["initials"]
        except KeyError:
            self.initials = _module_data["initials"] = {
                d["initials"]: d["frequency"]
                for d in MongoDB(f"{self.collection}_initials_occurrence").find()
            }

    def get_metrics(self) -> dict:
        self.calc_prob()
        self.get_response()
        self.counts = {
            k: round(v, 4)
            for k, v in self.counts.items()
        }
        return self.response

    def get_count_and_type(self, count_type: str) -> Tuple[int, str]:
        q = {"query": {"bool": {"filter": {"term": {
            "lastname.keyword": self.doc["details"]["lastname"]
        }}}}}
        res = self.esl.find(q, size=1, source_only=True)
        count_ = res[count_type] if res else 1
        count_ -= 1
        if count_type == "fuzzy" and not count_:
            raise MatchError(self.doc["_id"])
        suffix = "_fuzzy" if count_type == "fuzzy" else ""
        return count_, suffix

    def calc_prob(self):
        if self.doc["details"]["initials"]:
            # Get all the probabilities
            prob = [self.initials[i] for i in self.doc["details"]["initials"]]
            # Get total probability by muliplying
            result = prod(prob)
            for count_type in ("fuzzy", "count"):
                count_, suffix = self.get_count_and_type(count_type)
                self.counts[f"full{suffix}"] = count_ * result
                # Get first initial probability
                self.counts[f"first{suffix}"] = count_ * prob[0]
        else:
            for count_type in ("fuzzy", "count"):
                count_, suffix = self.get_count_and_type(count_type)
                for key in ("full", "first"):
                    self.counts[f"{key}{suffix}"] = count_

    def get_response(self):
        for key, count_ in self.counts.items():
            if count_ < self.alpha:
                self.response[key] = True
            else:
                self.response[key] = [
                    keys for keys, p in self.combinations.items()
                    if count_ * p < self.alpha]
