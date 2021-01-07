"""Access Matrixian Person Data.

Usage::
    from common.persons import Person
    person = Person(lastname="Saalbrink", initials="PP", postcode="1061BD")
    person.update()

    from common.persons import Address
    address = Address(postcode="1061BD", housenumber=145)
    person = address.upgrade()

Modifiers (with defaults)::
    set_alpha(alpha=.05)
    set_clean_email(clean_email=True)
    set_population_size(oldest_client_record_in_years=20)
    set_search_size(size=10)
    set_years_ago(years_ago=3)

Exceptions::
    MatchError
    NoMatch
    PersonsError
"""

from __future__ import annotations

__all__ = (
    "Address",
    "Constant",
    "Match",
    "MatchError",
    "Names",
    "NoMatch",
    "Person",
    "PersonsError",
    "Re",
    "Query",
    "Statistics",
    "parse_name",
    "preload_db",
    "set_alpha",
    "set_clean_email",
    "set_must_have_address",
    "set_population_size",
    "set_search_size",
    "set_years_ago",
)

from collections import namedtuple
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime, timedelta
from functools import lru_cache
from math import ceil
import re
from typing import Any, Optional, Union

try:  # python3.8
    from functools import cached_property
except ImportError:  # python3.7
    from cached_property import cached_property

from dateutil.parser import parse as dateparse
from text_unidecode import unidecode

import common.api.email as email
import common.api.phone as phone
from ._persons_probabilities import (
    estimated_people_with_lastname,
    extra_fields_calculation,
    get_name_counts,
    set_alpha,
    set_population_size,
)
from .connectors.mx_elastic import ESClient
from .exceptions import MatchError, NoMatch, PersonsError
from .parsers import levenshtein, DISTANCE

set_alpha = set_alpha
set_population_size = set_population_size


def set_clean_email(clean_email: bool = True) -> bool:
    Constant.CLEAN_EMAIL = clean_email
    return True


def set_must_have_address(must_have_address: bool = False) -> bool:
    Constant.MUST_HAVE_ADDRESS = must_have_address
    return True


def set_search_size(size: int = 10) -> bool:
    Constant.SEARCH_SIZE = size
    return True


def set_years_ago(years_ago: int = 3) -> bool:
    Constant.YEARS_AGO = Constant.TODAY - timedelta(days=365.25 * years_ago)
    return True


class Constant:
    CLEAN_EMAIL = True
    MUST_HAVE_ADDRESS = False
    SEARCH_SIZE = 10_000
    HIGH_SCORE = 1
    LOW_SCORE = 4
    YEAR_STEP = 3
    TODAY = datetime.now()
    CURR_YEAR = TODAY.year
    YEARS_AGO = TODAY - timedelta(days=365.25 * 3)
    ND_INDEX = "cdqc.names_data"
    PD_INDEX = "cdqc.person_data"
    DATE_FORMAT = "%Y-%m-%d"
    DEFAULT_DATE = "1900-01-01"
    EMPTY = {(): 0.}
    NAMES: Optional[Names] = None
    NAME = ("lastname", "initials", "gender", "firstname", "middlename")
    ADDRESS = ("postcode", "housenumber", "housenumber_ext", "street", "city", "country")
    OTHER = ("mobile", "number", "date_of_birth", "email_address")
    META = ("date", "source")
    MATCH_KEYS = ("name", "address", "gender", "mobile", "number", "date_of_birth", "email_address", "family")
    PERSON_META = (*NAME, *OTHER, *META)
    COPY_FAMILY = ("address", "number", "lastname", "middlename")
    COPY_PERSON = (*COPY_FAMILY, "initials", "gender", "firstname", "mobile", "date_of_birth", "email_address")


class Person:
    """Data class for persons.

    Example::
        from common.persons import Person

        person = Person(
            lastname="Saalbrink",
            initials="P",
            postcode="1071XB",
            housenumber=71,
            mobile="0649978891",
            date="2016-01-01",
        )
        person.update()
    """
    __slots__ = (
        *Constant.NAME,
        "address",
        *Constant.OTHER,
        *Constant.META,
        "_match",
        "_statistics",
    )

    def __init__(
            self,
            ln: str = None,
            it: str = None,
            ad: Address = None,
            *,
            lastname: str = None,
            initials: str = None,
            gender: str = None,
            firstname: str = None,
            middlename: str = None,
            postcode: str = None,
            housenumber: Union[str, int] = None,
            housenumber_ext: str = None,
            street: str = None,
            city: str = None,
            country: str = "NLD",
            mobile: str = None,
            number: str = None,
            date_of_birth: Union[str, datetime] = None,
            email_address: str = None,
            date: Union[str, datetime] = None,
            source: str = None,
    ):
        if ad is None:
            self.lastname = lastname
            self.initials = initials
            self.gender = gender
            self.firstname = firstname
            self.middlename = middlename
            self.address = Address(
                postcode,
                housenumber,
                housenumber_ext,
                street,
                city,
                country,
            )
            self.mobile = mobile
            self.number = number
            self.date_of_birth = date_of_birth
            self.email_address = email_address
            self.date = date
            self.source = source

        else:
            self.lastname = ln
            self.initials = it
            self.address = ad

        Cleaner(self)
        self._match: Optional[Match] = None
        self._statistics: Optional[Statistics] = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.lastname!r}, {self.initials!r}, {self.address!r})"

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        for attr in Constant.PERSON_META:
            yield attr, getattr(self, attr)
        yield from self.address

    def __eq__(self, other: Person) -> set:
        """Compare two `Person` objects.

        This method assumes left (`self`) as input and right (`other`) as output.
        """

        if self.lastname and other.lastname:
            distance = min(len(self.lastname), len(other.lastname))
            distance = 2 if distance > 5 else (1 if distance > 2 else 0)
        else:
            distance = 0

        def partial_initials_match():
            return self.initials.startswith(other.initials) or other.initials.startswith(self.initials)

        def partial_lastname_match():
            self_lastname = self.lastname.replace("ij", "y")
            other_lastname = other.lastname.replace("ij", "y")
            return (
                self_lastname in other_lastname or other_lastname in self_lastname
                or levenshtein(self_lastname, other_lastname, DISTANCE) <= distance
                or self_lastname == " ".join(reversed(other_lastname.split()))
            )

        if self.lastname and self.initials and other.lastname and other.initials:
            name = (
                (self.initials, self.lastname) == (other.initials, other.lastname)
                or (partial_initials_match() and partial_lastname_match())
            )
        elif self.lastname and other.lastname:
            name = self.lastname == other.lastname or partial_lastname_match()
        elif self.initials and other.initials:
            name = self.initials == other.initials or partial_initials_match()
        else:
            name = False

        if self.address.postcode and self.address.housenumber and self.address.housenumber_ext:
            address = (
                (self.address.postcode, self.address.housenumber, self.address.housenumber_ext)
                == (other.address.postcode, other.address.housenumber, other.address.housenumber_ext)
            )
        elif self.address.postcode and self.address.housenumber:
            address = ((self.address.postcode, self.address.housenumber)
                       == (other.address.postcode, other.address.housenumber))
        elif self.address.postcode:
            address = self.address.postcode == other.address.postcode
        elif self.address.housenumber:
            address = self.address.housenumber == other.address.housenumber
        else:
            address = False

        family = self.lastname and other.lastname and self.address.postcode and (
            (self.lastname == other.lastname and self.address.postcode == other.address.postcode)
            or (partial_lastname_match() and address)
        )

        gender = self.gender and self.gender == other.gender
        date_of_birth = self.date_of_birth and self.date_of_birth == other.date_of_birth
        mobile = self.mobile and self.mobile == other.mobile
        number = self.number and self.number == other.number
        email_address = self.email_address and self.email_address == other.email_address

        local_vars = locals()
        return {k for k in Constant.MATCH_KEYS if local_vars[k]}

    def __or__(self, other: Person) -> Person:
        """Update `self` with date from `other`.

        This method assumes left (`self`) as input and right (`other`) as output.
        """

        # Because of our probability calculation, we will only make two types of matches
        # The difference between the two is similarity of initials
        # This is enough to distinguish a person match from a family match
        person_match = not self.initials or (self.initials == other.initials or (
            other.initials and (
                self.initials.startswith(other.initials) or other.initials.startswith(self.initials))
        ))
        # Based on this, we only copy certain fields if there's a family match
        copy = Constant.COPY_PERSON if person_match else Constant.COPY_FAMILY

        if ((self.date and self.date < (other.date or Constant.YEARS_AGO))
                or (not self.date and Constant.YEARS_AGO < (other.date or Constant.YEARS_AGO))):

            # Overwrite all if other is more recent
            for attr in copy:
                value = getattr(other, attr)
                if value:
                    setattr(self, attr, value)

        else:

            # Only overwrite empty attributes
            for attr in copy:
                if not getattr(self, attr):
                    setattr(self, attr, getattr(other, attr))

        # Only overwrite empty attributes
        for attr in Constant.META:
            if not getattr(self, attr):
                setattr(self, attr, getattr(other, attr))

        return self

    def as_dict(self) -> dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        d = {}
        for attr in self.__slots__:
            value = getattr(self, attr)
            if hasattr(value, "as_dict"):
                value = value.as_dict()
            d[attr] = value
        return d

    @classmethod
    def from_address(cls, address: Address) -> Person:
        return cls(
            postcode=address.postcode,
            housenumber=address.housenumber,
            housenumber_ext=address.housenumber_ext,
            street=address.street,
            city=address.city,
            country=address.country,
        )

    @classmethod
    def from_doc(cls, doc: dict) -> Person:
        """Create a `Person` from an Elasticsearch response."""
        if "hits" in doc:
            doc = doc["hits"]["hits"][0]
        if "_source" in doc:
            doc = doc["_source"]
        return cls(
            lastname=doc["details"]["lastname"],
            initials=doc["details"]["initials"],
            gender=doc["details"]["gender"],
            postcode=doc["address"]["postalCode"],
            housenumber=doc["address"]["houseNumber"],
            housenumber_ext=doc["address"]["houseNumberExt"],
            street=doc["address"]["street"],
            city=doc["address"]["city"],
            country=doc["address"]["country"],
            mobile=doc["phoneNumber"]["mobile"],
            number=doc["phoneNumber"]["number"],
            date_of_birth=doc["birth"]["date"],
            email_address=doc["contact"]["email"],
            date=doc["date"],
            source=doc["source"],
        )

    @property
    def match(self) -> Match:
        if not self._match:
            if any(getattr(self, attr) for attr in Constant.PERSON_META):
                self._match = Match(self, query_type="person_query")
            else:
                self._match = Match(self, query_type="address_query")
        return self._match

    @property
    def statistics(self) -> Statistics:
        if not self._statistics:
            self._statistics = Statistics(self)
        return self._statistics

    def update(self) -> Person:
        """Update a `Person` with `Match`.

        After updating, access the `Match` object through `Person.match`.
        """
        if not self.statistics:
            raise PersonsError("Too few fields to reliably update this Person.")
        return self | self.match.composite


class Address:
    """Data class for addresses."""
    __slots__ = Constant.ADDRESS

    def __init__(
            self,
            postcode: str = None,
            housenumber: int = None,
            housenumber_ext: str = None,
            street: str = None,
            city: str = None,
            country: str = "NLD",
    ):
        self.postcode = postcode
        self.housenumber = housenumber
        self.housenumber_ext = housenumber_ext
        self.street = street
        self.city = city
        self.country = country

    def __bool__(self) -> bool:
        return bool(self.postcode and self.housenumber)

    def __repr__(self) -> str:
        if self.housenumber_ext:
            return f"{type(self).__name__}({self.postcode!r}, {self.housenumber!r}, {self.housenumber_ext!r})"
        else:
            return f"{type(self).__name__}({self.postcode!r}, {self.housenumber!r})"

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        for attr in Constant.ADDRESS:
            yield attr, getattr(self, attr)

    @property
    def address_id(self) -> str:
        return f"{self.postcode or ''} {self.housenumber or ''} {self.housenumber_ext or ''}".strip()

    def as_dict(self) -> dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {attr: getattr(self, attr) for attr in self.__slots__}

    def upgrade(self) -> Person:
        """Upgrade an `Address` to a `Person`."""
        person = Person.from_address(self)
        return person | person.match.composite


Matchable = Union[Person, Address]


class Statistics:
    __slots__ = (
        "estimation",
        "extra_fields",
        "_bool",
    )

    def __init__(self, person: Person):
        self.estimation = int(estimated_people_with_lastname(person.lastname))
        try:
            self.extra_fields = extra_fields_calculation(
                lastname=person.lastname,
                initials=person.initials,
                must_have_address=Constant.MUST_HAVE_ADDRESS,
                fuzzy_address=person.address,
                address=person.address,
                postcode=None if person.address else person.address.postcode,
                date_of_birth=person.date_of_birth,
                mobile=person.mobile,
                number=person.number,
            )
            self._bool = True
        except PersonsError:
            self.extra_fields = {}
            self._bool = False

    def __bool__(self) -> bool:
        return self._bool

    def __repr__(self) -> str:
        return f"{type(self).__name__}"

    def as_dict(self) -> dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {attr: getattr(self, attr) for attr in self.__slots__}


class Names:
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
        self.es = ESClient(Constant.ND_INDEX, index_exists=True)

    def __repr__(self) -> str:
        return f"{type(self).__name__}"

    @cached_property
    def affixes(self) -> set[str]:
        """Load a set with affixes.

        The output can be used to clean last name data.
        """
        return {doc["_source"]["affix"] for doc in self.es.findall(
            {"query": {"bool": {"must": {"term": {"data": "affixes"}}}}}
        )}

    @cached_property
    def first_names(self) -> dict[str, str]:
        """Load a dictionary with first names and gender occurrence.

        This function returns if any given Dutch first name has more
        male or female bearers. If the number is equal, None is
        returned. Names are cleaned before output.

        The output can be used to fill missing gender data.
        """
        return {doc["_source"]["firstname"]: doc["_source"]["gender"] for doc in  # noqa
                self.es.findall(
                    {"query": {"bool": {"must": {"term": {"data": "firstnames"}}}}})}

    @cached_property
    def titles(self) -> set[str]:
        """Load a set with titles.

        The output can be used to clean last name data.
        """
        return set(doc["_source"]["title"] for doc in  # noqa
                   self.es.findall(
                       {"query": {"bool": {"must": {"term": {"data": "titles"}}}}}))

    @cached_property
    def surnames(self) -> dict[str, int]:
        """Load a dictionary with surnames and their numbers.

        The output can be used for data and matching quality calculations.
        """
        return {doc["_source"]["surname"]: doc["_source"]["number"]  # noqa
                for doc in self.es.findall(
                {"query": {"bool": {"must": {"term": {"data": "surnames"}}}}})}


Constant.NAMES = Names()
ParsedName = namedtuple("ParsedName", ("first", "last", "gender"))


def preload_db():
    """Helper function to aid application startup."""
    _ = Constant.NAMES.affixes
    _ = Constant.NAMES.first_names


def parse_name(name: str) -> ParsedName:
    """Parse parts from a name."""

    for char in "_.+-":
        name = name.replace(char, " ")
    name_split = [token.title() for token in name.split()]

    initials = []
    for token in name_split.copy():
        if len(token) == 1:
            name_split.remove(token)
            initials.append(token)
        else:
            break

    affixes = []
    for token in name_split.copy():
        token_lower = token.lower()
        if token_lower in Constant.NAMES.affixes:
            name_split.remove(token)
            affixes.append(token_lower)

    last_names = []
    first_names = []
    for token in name_split:
        first_name_count, last_name_count = get_name_counts(token)
        if last_name_count > first_name_count:
            last_names.append(token)
        else:
            first_names.append(token)

    # Make sure we have a last name instead of multiple first names
    if not last_names and len(first_names) > 1:
        last_names.append(first_names.pop())

    if last_names:
        last_names = affixes + last_names

    first_name = " ".join(first_names) or None
    gender = Constant.NAMES.first_names.get(first_names[0]) if first_names else None
    initials = ".".join(initials) + "." if initials else None
    last_name = " ".join(last_names) or None

    return ParsedName(initials or first_name, last_name, gender)


class Re:
    initials = re.compile(r"[^A-Za-z\u00C0-\u017F]")
    hn = re.compile(r"[^0-9]")
    hne1 = re.compile(r"[^A-Za-z0-9\u00C0-\u017F]")
    hne2 = re.compile(r"\D+(?=\d)")
    name = re.compile(r"[^\sA-Za-z\u00C0-\u017F]")
    single_char = re.compile(r"(?<!\S)\S(?!\S)")
    hyphen = re.compile(r"-")
    firstname = re.compile(r"(^[A-Za-z\u00C0-\u017F-]+)")
    whitespace = re.compile(r"\s{2,}")


class Cleaner:
    """Cleaner for Data objects."""
    affixes = [f"{aff.title()} " for aff in Constant.NAMES.affixes | Constant.NAMES.titles]
    countries = {"nederland", "netherlands", "nl", "nld"}
    genders = {"MAN": "M", "VROUW": "V"}
    date_fields = ("date", "date_of_birth")
    phone_fields = ("number", "mobile")
    person: Optional[Person] = None

    def __init__(self, person: Person):
        self.person = person
        self.clean()

    def __repr__(self) -> str:
        return f"{type(self).__name__}"

    def clean(self):
        self.check_country()
        self.clean_dates()
        if Constant.CLEAN_EMAIL:
            self.clean_email()
        self.clean_gender()
        self.clean_hn()
        self.clean_hne()
        self.clean_initials()
        self.clean_lastname()
        self.clean_postcode()
        self.clean_phones()

    def check_country(self):
        """Check if input country is accepted."""
        if self.person.address.country.lower() not in self.countries:
            raise PersonsError(f"Not implemented for country {self.person.address.country}.")

    def clean_dates(self):
        """Clean and parse dates."""
        for attr in self.date_fields:
            date = getattr(self.person, attr)
            if isinstance(date, str):
                date = date.split()[0]
                try:
                    date = datetime.strptime(date[:10], Constant.DATE_FORMAT)
                except ValueError:
                    try:
                        date = dateparse(date, ignoretz=True)
                    except ValueError:
                        date = None
                setattr(self.person, attr, date)
            if isinstance(date, datetime) and f"{date.date()}" == Constant.DEFAULT_DATE:
                setattr(self.person, attr, None)

    def clean_email(self):
        if isinstance(self.person.email_address, str):
            response = email.check_email(self.person.email_address)
            if response["safe_to_send"]:
                self.person.email_address = response["email"]
            else:
                self.person.email_address = None

    def clean_gender(self):
        """Clean gender."""
        if not isinstance(self.person.gender, str):
            self.person.gender = None
            return
        self.person.gender = self.person.gender.upper()
        if self.person.gender in self.genders.values():
            return
        self.person.gender = self.genders.get(self.person.gender)

    def clean_hn(self):
        """Clean house number."""
        if isinstance(self.person.address.housenumber, str):
            for d in ("/", "-"):
                if d in self.person.address.housenumber:
                    self.person.address.housenumber = self.person.address.housenumber.split(d)[0]
            self.person.address.housenumber = Re.hn.sub("", self.person.address.housenumber)
        try:
            self.person.address.housenumber = int(float(self.person.address.housenumber))
        except (TypeError, ValueError):
            self.person.address.housenumber = None

    def clean_hne(self):
        """Clean house number extension."""
        if isinstance(self.person.address.housenumber_ext, str):
            self.person.address.housenumber_ext = Re.hne2.sub(
                "", Re.hne1.sub("", self.person.address.housenumber_ext.upper()))

    def clean_initials(self):
        """Clean initials."""
        if isinstance(self.person.initials, str):
            self.person.initials = Re.initials.sub("", self.person.initials.upper())

    def clean_lastname(self):
        """Clean last name.

        Keep only letters; hyphens become spaces.
        Remove all special characters and titles.
        """
        lastname = self.person.lastname
        if isinstance(lastname, str):
            lastname = lastname.title()
            for o in self.affixes:
                lastname = lastname.replace(o, "")
                if " " not in lastname:
                    break
            self.person.lastname = unidecode(Re.single_char.sub(
                "", Re.whitespace.sub(" ", Re.name.sub("", Re.hyphen.sub(" ", lastname)))
            ).strip())

    def clean_postcode(self):
        """Clean postal code."""
        if isinstance(self.person.address.postcode, str):
            self.person.address.postcode = self.person.address.postcode.replace(" ", "").upper()
            if len(self.person.address.postcode) != 6 or self.person.address.postcode.startswith("0"):
                self.person.address.postcode = None

    def clean_phones(self):
        """Clean and parse phone and mobile numbers."""
        for input_type in self.phone_fields:
            number = getattr(self.person, input_type)
            if number:
                try:
                    parsed = phone.check_phone(number, self.person.address.country, call=True)
                    if parsed.valid_number:
                        try:
                            parsed.number_type = parsed.number_type.replace("landline", "number")
                        except AttributeError:
                            parsed.number_type = input_type
                        setattr(self.person, parsed.number_type, parsed.parsed_number)
                        if input_type != parsed.number_type:
                            setattr(self.person, input_type, None)
                    else:
                        setattr(self.person, input_type, None)
                except phone.PhoneApiError:
                    setattr(self.person, input_type, None)


class Query:
    __slots__ = (
        "_address",
        "_address_query",
        "_date_of_birth",
        "_email_address",
        "_fuzzy_address",
        "_gender",
        "_initials",
        "_lastname",
        "_mobile",
        "_number",
        "_person_query",
        "_postcode",
        "_query",
        "_repr",
        "person",
    )
    clauses = (
        "lastname",
        "initials",
        "gender",
        "date_of_birth",
        "address",
        "fuzzy_address",
        "postcode",
        "email_address",
        "mobile",
        "number",
    )
    sort = [{"date": "desc"}, {"_score": "desc"}]

    def __init__(self, matchable: Matchable):
        self._address = self._address_query = self._date_of_birth = self._email_address \
            = self._fuzzy_address = self._gender = self._initials = self._lastname = self._mobile \
            = self._number = self._person_query = self._postcode = self._query = self._repr = None
        if isinstance(matchable, Person):
            self.person = matchable
        elif isinstance(matchable, Address):
            self.person = Person.from_address(matchable)

    def __repr__(self) -> str:
        if not self._repr:
            if self.person.lastname or self.person.initials:
                self._repr = f"{self.person_query}"
            else:
                self._repr = f"{self.address_query}"
        return self._repr

    def as_dict(self) -> dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {attr: getattr(self, attr) for attr in self.__slots__}

    @property
    def lastname_clause(self) -> dict:
        if not self._lastname:
            clause = [
                {"query": self.person.lastname, "fuzziness": "AUTO"},
            ]
            if "ij" in self.person.lastname:
                clause.append({"query": self.person.lastname.replace("ij", "y"), "fuzziness": "AUTO"})
            elif "y" in self.person.lastname:
                clause.append({"query": self.person.lastname.replace("y", "ij"), "fuzziness": "AUTO"})
            clause = [{"match": {"details.lastname": q}} for q in clause]
            clause.append({"match": {"details.lastname.keyword": {"query": self.person.lastname, "boost": 2}}})
            self._lastname = {"bool": {"should": clause, "minimum_should_match": 1}}
        return self._lastname

    @property
    def initials_clause(self) -> dict:
        if not self._initials:
            self._initials = {"bool": {"should": [
                {"term": {"details.initials.keyword": self.person.initials[:i]}}
                for i in range(1, len(self.person.initials) + 1)
            ], "minimum_should_match": 1, "boost": 2}}
        return self._initials

    @staticmethod
    @lru_cache()
    def _get_lastname_clause(lastname: str, fuzzy: str):
        return {"bool": {"should": [
            *[{"match": {"details.lastname.keyword": q}} for q in (
                {"query": lastname, "fuzziness": fuzzy},
                {"query": lastname.replace("ij", "y"), "fuzziness": fuzzy}
                if "ij" in lastname else None,
                {"query": lastname.replace("y", "ij"), "fuzziness": fuzzy}
                if "y" in lastname else None,
            ) if q],
            {"match": {"details.lastname.keyword": {"query": lastname, "boost": 2}}},
        ], "minimum_should_match": 1}}

    @lru_cache()
    def get_lastname_clause(self, lastname: str, fuzzy: str) -> Optional[dict]:
        fuzzy = "AUTO" if fuzzy == "fuzzy" else 0
        if isinstance(lastname, str):
            return self._get_lastname_clause(lastname, fuzzy)
        elif isinstance(lastname, tuple):
            return {"bool": {"should": [
                self._get_lastname_clause(name, fuzzy)
                for name in lastname
            ], "minimum_should_match": 1}}

    @lru_cache()
    def get_initials_clause(self, initials: str) -> Optional[dict]:
        if initials:
            if len(initials) == len(self.person.initials):
                return {"bool": {"should": [
                    {"bool": {"must": {"term": {"details.initials.keyword": initials}}, "boost": 2}},
                    {"wildcard": {"details.initials": initials.lower() + "*"}},
                ], "minimum_should_match": 1, "boost": 2}}
            else:
                return {"bool": {"should": [
                    {"term": {"details.initials.keyword": self.person.initials[:i]}}
                    for i in range(1, len(self.person.initials) + 1)
                ], "minimum_should_match": 1, "boost": 2}}

    @property
    def gender_clause(self) -> dict:
        if not self._gender:
            self._gender = {"term": {"details.gender.keyword": self.person.gender}}
        return self._gender

    @property
    def date_of_birth_clause(self) -> dict:
        if not self._date_of_birth:
            self._date_of_birth = {"term": {"birth.date": self.person.date_of_birth}}
        return self._date_of_birth

    @property
    def address_clause(self) -> dict:
        if not self._address:
            self._address = {"bool": {"should": [
                {"match": {"address.address_id.keyword": {
                    "query": self.person.address.address_id,
                    "boost": 2,
                }}},
                {"bool": {"must": [
                    {"term": {"address.postalCode.keyword": self.person.address.postcode}},
                    {"term": {"address.houseNumber": self.person.address.housenumber}},
                ]}},
            ], "minimum_should_match": 1}}
        return self._address

    @property
    def fuzzy_address_clause(self) -> dict:
        if not self._fuzzy_address:
            self._fuzzy_address = {"match": {"address.address_id.keyword": {
                    "query": self.person.address.address_id,
                    "fuzziness": 1,
                }}}
        return self._fuzzy_address

    @property
    def postcode_clause(self) -> dict:
        if not self._postcode:
            self._postcode = {"term": {"address.postalCode.keyword": self.person.address.postcode}}
        return self._postcode

    @property
    def email_address_clause(self) -> dict:
        if not self._email_address:
            self._email_address = {"term": {"contact.email": self.person.email_address}}
        return self._email_address

    @property
    def mobile_clause(self) -> dict:
        if not self._mobile:
            self._mobile = {"term": {"phoneNumber.mobile": self.person.mobile.replace("+31", "")}}
        return self._mobile

    @property
    def number_clause(self) -> dict:
        if not self._number:
            self._number = {"term": {"phoneNumber.number": self.person.number.replace("+31", "")}}
        return self._number

    @property
    def address_query(self) -> dict:
        """This is a query that will match only on Address."""
        if not self._address_query:
            self._address_query = {"query": {"match": {
                "address.address_id.keyword": self.person.address.address_id,
            }}, "sort": self.sort}
        return self._address_query

    @property
    def person_query(self) -> dict:
        """This is a calculated query that will match only this Person."""
        if not self._person_query:
            self._person_query = {"query": {"bool": {"should": [
                {"bool": {"must": [
                    clause for clause in (
                        self.get_lastname_clause(lastname, situation),
                        self.get_initials_clause(initials),
                        *(getattr(self, clause + "_clause")
                          for clause in extra_fields),
                    ) if clause
                ]}}
                for lastname, initials, situation, *extra_fields in self.person.statistics.extra_fields
            ]}}, "sort": self.sort}
        return self._person_query

    @property
    def query(self) -> dict:
        """This is a complete query that matches everything, no probability calculations."""
        if not self._query:
            self._query = {"query": {"bool": {"should": [
                getattr(self, clause + "_clause")
                for clause in self.clauses
                if (getattr(self.person.address, clause)
                    if clause == "postcode"
                    else (self.person.address
                          if clause == "fuzzy_address"
                          else getattr(self.person, clause)))
            ]}}, "sort": self.sort}
        return self._query


def score(year: int) -> str:
    """Gives a score (1: best, 4: worst) based on the date."""
    c = Constant
    return f"{min(c.LOW_SCORE, ceil((c.CURR_YEAR - year + c.HIGH_SCORE) / c.YEAR_STEP))}"


class Match:
    __slots__ = (
        "_composite",
        "_matches",
        "_match_keys",
        "_match_score",
        "_query_type",
        "_search_response",
        "person",
        "query",
    )

    def __init__(self, matchable: Matchable, query_type: str = "person_query"):
        self._composite: Optional[Person] = None
        self._matches: Optional[list[Person]] = None
        self._match_keys: set[str] = set()
        self._match_score: Optional[str] = None
        self._query_type = query_type
        self._search_response: Optional[list[dict]] = None
        matchable = deepcopy(matchable)
        if isinstance(matchable, Person):
            self.person = matchable
        elif isinstance(matchable, Address):
            self.person = Person.from_address(matchable)
        self.query = Query(matchable)

    def __repr__(self) -> str:
        return f"{type(self).__name__}"

    def as_dict(self) -> dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {attr: getattr(self, attr) for attr in self.__slots__}

    @property
    def search_response(self) -> list[dict]:
        """Elastic response for this Match."""
        if not self._search_response:
            self._search_response = ESClient(
                Constant.PD_INDEX,
                index_exists=True,
            ).search(
                index=Constant.PD_INDEX,
                body=getattr(self.query, self._query_type),
                size=Constant.SEARCH_SIZE,
            )["hits"]["hits"]
            if not self._search_response:
                raise NoMatch
        return self._search_response

    @property
    def matches(self) -> list[Person]:
        if not self._matches:
            self._matches = [
                Person.from_doc(doc)
                for doc in self.search_response
            ]
        return self._matches

    @property
    def match_keys(self) -> set[str]:
        if not self._match_keys:
            self._match_keys |= self.person == self.composite
            if not self._match_keys:
                raise NoMatch
        return self._match_keys

    @property
    def match_score(self) -> str:
        if not self._match_score:
            n_match_keys = len(self.match_keys)
            try:
                date_score = score(self.composite.date.year)
            except AttributeError:
                date_score = "4"
            if n_match_keys >= 4:
                self._match_score = "A" + date_score
            elif n_match_keys == 3:
                self._match_score = "B" + date_score
            elif n_match_keys == 2:
                self._match_score = "C" + date_score
            elif n_match_keys == 1:
                self._match_score = "D" + date_score
            else:
                raise MatchError
        return self._match_score

    @property
    def composite(self) -> Person:
        """Create a composite output `Person`."""
        if not self._composite:
            self._composite = deepcopy(self.matches[0])
            for person in self.matches[1:]:
                self._composite |= person
        return self._composite
