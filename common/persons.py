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

Exceptions::
    MatchError
    NoMatch
    PersonsError
"""

from collections import deque
from datetime import datetime
from math import ceil
import re
from typing import Any, Deque, Dict, List, Optional, Set, Union

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
    set_alpha,
    set_population_size,
)
from .connectors.mx_elastic import ESClient
from .exceptions import MatchError, NoMatch, PersonsError

set_alpha = set_alpha
set_population_size = set_population_size


def set_clean_email(clean_email: bool = True) -> bool:
    Constant.CLEAN_EMAIL = clean_email
    return True


def set_search_size(size: int = 10) -> bool:
    Constant.SEARCH_SIZE = size
    return True


class Constant:
    CLEAN_EMAIL = True
    SEARCH_SIZE = 10
    HIGH_SCORE = 1
    LOW_SCORE = 4
    YEAR_STEP = 3
    CURR_YEAR = datetime.now().year
    ND_INDEX = "cdqc.names_data"
    PD_INDEX = "cdqc.person_data"
    DATE_FORMAT = "%Y-%m-%d"
    DEFAULT_DATE = "1900-01-01"
    EMPTY = {(): 0.}
    NAME = ("lastname", "initials", "gender", "firstname", "middlename")
    ADDRESS = ("postcode", "housenumber", "housenumber_ext", "street", "city", "country")
    OTHER = ("mobile", "number", "date_of_birth", "email_address")
    META = ("date", "source")
    MATCH_KEYS = ("name", "address", "gender", "mobile", "number", "date_of_birth", "email_address")


class Person:
    """Data class for persons."""
    __slots__ = (
        *Constant.NAME,
        "address",
        *Constant.OTHER,
        *Constant.META,
        "match",
        "statistics",
    )

    def __init__(
            self,
            lastname: str = None,
            initials: str = None,
            gender: str = None,
            firstname: str = None,
            middlename: str = None,
            postcode: str = None,
            housenumber: int = None,
            housenumber_ext: str = None,
            street: str = None,
            city: str = None,
            country: str = "NLD",
            mobile: str = None,
            number: str = None,
            date_of_birth: datetime = None,
            email_address: str = None,
            date: datetime = None,
            source: str = None,
    ):
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

        Cleaner().clean(self)
        self.match: Optional[Match] = None
        self.statistics = Statistics(self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.lastname!r}, {self.initials!r}, {self.address!r})"

    def __eq__(self, other: "Person") -> set:
        """Compare two `Person` objects.

        This method assumes left (`self`) as input and right (`other`) as output.
        """
        if self.lastname and self.initials:
            name = (
                    f"{self.initials} {self.lastname}" == f"{other.initials} {other.lastname}"
                    or ((self.initials in other.initials or other.initials in self.initials)
                        and (self.lastname in other.lastname or other.lastname in self.lastname))
            )
        elif self.lastname:
            name = self.lastname == other.lastname
        elif self.initials:
            name = self.initials == other.initials
        else:
            name = False

        if self.address.postcode and self.address.housenumber and self.address.housenumber_ext:
            address = (
                    f"{self.address.postcode} {self.address.housenumber} {self.address.housenumber_ext}"
                    == f"{other.address.postcode} {other.address.housenumber} {other.address.housenumber_ext}"
            )
        elif self.address.postcode and self.address.housenumber:
            address = (f"{self.address.postcode} {self.address.housenumber}"
                       == f"{other.address.postcode} {other.address.housenumber}")
        elif self.address.postcode:
            address = self.address.postcode == other.address.postcode
        elif self.address.housenumber:
            address = self.address.housenumber == other.address.housenumber
        else:
            address = False

        gender = self.gender and self.gender == other.gender
        date_of_birth = self.date_of_birth and self.date_of_birth == other.date_of_birth
        mobile = self.mobile and self.mobile == other.mobile
        number = self.number and self.number == other.number
        email_address = self.email_address and self.email_address == other.email_address

        local_vars = locals()
        return {k for k in Constant.MATCH_KEYS if local_vars[k]}

    def __or__(self, other: "Person") -> "Person":
        """Update `self` with date from `other`.

        This method assumes left (`self`) as input and right (`other`) as output.
        """
        if self.date and other.date and self.date < other.date:
            # Overwrite if other is more recent
            for attr in other.__slots__:
                if getattr(other, attr):
                    setattr(self, attr, getattr(other, attr))
        else:
            for attr in (*Constant.NAME, *Constant.OTHER):
                # Only overwrite empty attributes
                if not getattr(self, attr):
                    setattr(self, attr, getattr(other, attr))
            if not self.address.postcode or not self.address.housenumber:
                # Overwrite complete address
                self.address = other.address
            elif (self.address.postcode == other.address.postcode
                  and self.address.housenumber == other.address.housenumber):
                # Only overwrite empty attributes
                for attr in Constant.ADDRESS:
                    if not getattr(self.address, attr):
                        setattr(self.address, attr, getattr(other.address, attr))
        for attr in Constant.META:
            # Only overwrite empty attributes
            if not getattr(self, attr):
                setattr(self, attr, getattr(other, attr))
        return self

    def as_dict(self) -> Dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {
            **{attr: getattr(self, attr) for attr in self.__slots__},
            **self.address.as_dict(),
        }

    @classmethod
    def from_address(cls, address: "Address") -> "Person":
        return cls(
            postcode=address.postcode,
            housenumber=address.housenumber,
            housenumber_ext=address.housenumber_ext,
            street=address.street,
            city=address.city,
            country=address.country,
        )

    @classmethod
    def from_doc(cls, doc: dict) -> "Person":
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

    def update(self) -> "Person":
        """Update a `Person` with `Match`.

        After updating, access the `Match` object through `Person.match`.
        """
        if not self.statistics:
            raise PersonsError("Too few fields to reliably update this Person.")
        self.match = Match(self)
        return self | self.match.composite


class Address:
    """Data class for addresses."""
    __slots__ = (
        *Constant.ADDRESS,
    )

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

    @property
    def address_id(self) -> str:
        return f"{self.postcode or ''} {self.housenumber or ''} {self.housenumber_ext or ''}".strip()

    def as_dict(self) -> Dict[str, Any]:
        """Create a dictionary from all attributes (similar to __dict__)."""
        return {attr: getattr(self, attr) for attr in self.__slots__}

    def upgrade(self) -> Person:
        """Upgrade an `Address` to a `Person`."""
        person = Person.from_address(self)
        person.match = Match(person, query_type="address_query")
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

    @cached_property
    def affixes(self) -> Set[str]:
        """Load a set with affixes.

        The output can be used to clean last name data.
        """
        return {doc["_source"]["affix"] for doc in self.es.findall(
            {"query": {"bool": {"must": {"term": {"data": "affixes"}}}}}
        )}

    @cached_property
    def first_names(self) -> Dict[str, str]:
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
    def titles(self) -> Set[str]:
        """Load a set with titles.

        The output can be used to clean last name data.
        """
        return set(doc["_source"]["title"] for doc in  # noqa
                   self.es.findall(
                       {"query": {"bool": {"must": {"term": {"data": "titles"}}}}}))

    @cached_property
    def surnames(self) -> Dict[str, int]:
        """Load a dictionary with surnames and their numbers.

        The output can be used for data and matching quality calculations.
        """
        return {doc["_source"]["surname"]: doc["_source"]["number"]  # noqa
                for doc in self.es.findall(
                {"query": {"bool": {"must": {"term": {"data": "surnames"}}}}})}


Constant.NAMES = Names()


class Cleaner:
    """Cleaner for Data objects."""
    affixes = ("Van ", "Het ", "De ")
    countries = {"nederland", "netherlands", "nl", "nld"}
    genders = {"MAN": "M", "VROUW": "V"}
    date_fields = ("date", "date_of_birth")
    phone_fields = ("number", "mobile")
    person: Optional[Person] = None
    re_hn = re.compile(r"[^0-9]")
    re_hne1 = re.compile(r"[^A-Za-z0-9\u00C0-\u017F]")
    re_hne2 = re.compile(r"\D+(?=\d)")
    re_initials = re.compile(r"[^A-Za-z\u00C0-\u017F]")
    re_ln1 = re.compile(r"-")
    re_ln2 = re.compile(r"[^\sA-Za-z\u00C0-\u017F]")

    def clean(self, person: Person):
        self.person = person
        self.check_country()
        self.clean_dates()
        if Constant.CLEAN_EMAIL:
            self.clean_email()
        self.clean_gender()
        self.clean_hn()
        self.clean_hne()
        self.clean_initials()
        self.clean_lastname()
        self.clean_pc()
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
            self.person.address.housenumber = self.re_hn.sub("", self.person.address.housenumber)
        try:
            self.person.address.housenumber = int(float(self.person.address.housenumber))
        except (TypeError, ValueError):
            self.person.address.housenumber = None

    def clean_hne(self):
        """Clean house number extension."""
        if isinstance(self.person.address.housenumber_ext, str):
            self.person.address.housenumber_ext = self.re_hne1.sub("", self.person.address.housenumber_ext.upper())
            self.person.address.housenumber_ext = self.re_hne2.sub("", self.person.address.housenumber_ext)

    def clean_initials(self):
        """Clean initials."""
        if isinstance(self.person.initials, str):
            self.person.initials = self.re_initials.sub("", self.person.initials.upper())

    def clean_lastname(self):
        """Clean last name.

        Keep only letters; hyphens become spaces.
        Remove all special characters and titles.
        """
        if isinstance(self.person.lastname, str):
            self.person.lastname = self.person.lastname.title()
            for o in self.affixes:
                self.person.lastname = self.person.lastname.replace(o, "")
            self.person.lastname = self.re_ln1.sub(" ", self.person.lastname)
            self.person.lastname = self.re_ln2.sub("", self.person.lastname)
            self.person.lastname = unidecode(self.person.lastname.strip())
            if self.person.lastname and self.person.lastname.split()[-1].lower() in Constant.NAMES.titles:
                self.person.lastname = " ".join(self.person.lastname.split()[:-1])

    def clean_pc(self):
        """Clean postal code."""
        if isinstance(self.person.address.postcode, str):
            self.person.address.postcode = self.person.address.postcode.replace(" ", "").upper()
            if len(self.person.address.postcode) != 6 or self.person.address.postcode.startswith("0"):
                self.person.address.postcode = None

    def clean_phones(self):
        """Clean and parse phone and mobile numbers."""
        for input_type in self.phone_fields:
            number = getattr(self.person, input_type, None)
            try:
                parsed = phone.check_phone(number, self.person.address.country, call=True)
                if parsed.valid_number:
                    parsed.number_type = parsed.number_type.replace("landline", "number")
                    setattr(self.person, parsed.number_type, parsed.parsed_number)
                    if input_type != parsed.number_type:
                        setattr(self.person, input_type, None)
                else:
                    setattr(self.person, input_type, None)
            except phone.PhoneApiError:
                setattr(self.person, input_type, None)


class Query:
    __slots__ = (
        "person",
    )
    clauses = (
        "lastname",
        "initials",
        "gender",
        "date_of_birth",
        "address",
        "postcode",
        "email_address",
        "mobile",
        "number",
    )
    sort = [{"date": "desc"}, {"_score": "desc"}]

    def __init__(self, matchable: Matchable):
        if isinstance(matchable, Person):
            self.person = matchable
        elif isinstance(matchable, Address):
            self.person = Person.from_address(matchable)

    @property
    def lastname_clause(self) -> dict:
        clause = [
            {"query": self.person.lastname, "fuzziness": "AUTO"},
        ]
        if "ij" in self.person.lastname:
            clause.append({"query": self.person.lastname.replace("ij", "y"), "fuzziness": "AUTO"})
        elif "y" in self.person.lastname:
            clause.append({"query": self.person.lastname.replace("y", "ij"), "fuzziness": "AUTO"})
        clause = [{"match": {"details.lastname": q}} for q in clause]
        clause.append({"match": {"details.lastname.keyword": {"query": self.person.lastname, "boost": 2}}})
        return {"bool": {"should": clause, "minimum_should_match": 1}}

    @property
    def initials_clause(self) -> dict:
        return {"bool": {"should": [
            {"term": {"details.initials.keyword": self.person.initials[:i]}}
            for i in range(1, len(self.person.initials) + 1)
        ], "minimum_should_match": 1, "boost": 2}}

    @staticmethod
    def get_lastname_clause(lastname: str, fuzzy: str) -> Optional[dict]:
        if lastname:
            fuzzy = "AUTO" if fuzzy == "fuzzy" else 0
            clause = [
                {"query": lastname, "fuzziness": fuzzy},
            ]
            if "ij" in lastname:
                clause.append({"query": lastname.replace("ij", "y"), "fuzziness": fuzzy})
            elif "y" in lastname:
                clause.append({"query": lastname.replace("y", "ij"), "fuzziness": fuzzy})
            clause = [{"match": {"details.lastname": q}} for q in clause]
            clause.append({"match": {"details.lastname.keyword": {"query": lastname, "boost": 2}}})
            return {"bool": {"should": clause, "minimum_should_match": 1}}

    def get_initials_clause(self, initials: str) -> Optional[dict]:
        if initials:
            if len(initials) == len(self.person.initials):
                return {"bool": {"should": [
                    {"bool": {"must": {"term": {"details.initials.keyword": initials}}, "boost": 2}},
                    {"wildcard": {"details.initials": f"{initials.lower()}*"}},
                ], "minimum_should_match": 1, "boost": 2}}
            else:
                return {"bool": {"should": [
                    {"term": {"details.initials.keyword": self.person.initials[:i]}}
                    for i in range(1, len(self.person.initials) + 1)
                ], "minimum_should_match": 1, "boost": 2}}

    @property
    def gender_clause(self) -> dict:
        return {"bool": {"must_not": {"term": {
            "details.gender.keyword": "M" if self.person.gender == "V" else "V"
        }}}}

    @property
    def date_of_birth_clause(self) -> dict:
        return {"bool": {"should": [
            {"term": {"birth.date": self.person.date_of_birth}},
            {"term": {"birth.date": Constant.DEFAULT_DATE}},
        ], "minimum_should_match": 1}}

    @property
    def address_clause(self) -> dict:
        return {"bool": {"should": [
            {"match": {"address.address_id.keyword": {
                "query": self.person.address.address_id,
                "boost": 2}}},
            {"bool": {"must": [
                {"term": {"address.postalCode.keyword": self.person.address.postcode}},
                {"term": {"address.houseNumber": self.person.address.housenumber}},
            ]}},
        ], "minimum_should_match": 1}}

    @property
    def postcode_clause(self) -> dict:
        return {"term": {"address.postalCode.keyword": self.person.address.postcode}}

    @property
    def email_address_clause(self) -> dict:
        return {"term": {"contact.email": self.person.email_address}}

    @property
    def mobile_clause(self) -> dict:
        return {"term": {"phoneNumber.mobile": self.person.mobile.replace("+31", "")}}

    @property
    def number_clause(self) -> dict:
        return {"term": {"phoneNumber.number": self.person.number.replace("+31", "")}}

    @property
    def address_query(self) -> dict:
        return {"query": {"match": {
            "address.address_id.keyword": self.person.address.address_id,
        }}, "sort": self.sort}

    @property
    def person_query(self) -> dict:
        return {"query": {"bool": {"should": [
            {"bool": {"must": [
                clause for clause in (
                    self.get_lastname_clause(lastname, situation),
                    self.get_initials_clause(initials),
                    *(getattr(self, f"{clause}_clause")
                      for clause in extra_fields),
                ) if clause
            ]}}
            for lastname, initials, situation, *extra_fields in self.person.statistics.extra_fields
        ]}}, "sort": self.sort}

    @property
    def query(self) -> dict:
        return {"query": {"bool": {"should": [
            getattr(self, f"{clause}_clause")
            for clause in self.clauses
            if getattr(self.person, clause)
        ]}}, "sort": self.sort}

    def __repr__(self) -> str:
        if self.person.lastname or self.person.initials:
            return f"{self.person_query}"
        else:
            return f"{self.address_query}"


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
        self._matches: Optional[Deque[Person]] = None
        self._match_keys: Set[str] = set()
        self._match_score: Optional[str] = None
        self._query_type = query_type
        self._search_response: Optional[List[dict]] = None
        if isinstance(matchable, Person):
            self.person = matchable
        elif isinstance(matchable, Address):
            self.person = Person.from_address(matchable)
        self.query = Query(matchable)

    @property
    def search_response(self) -> List[dict]:
        if not self._search_response:
            self._search_response = ESClient(
                Constant.PD_INDEX,
                index_exists=True,
            ).search(
                index=Constant.PD_INDEX,
                body=getattr(self.query, self._query_type),
                size=Constant.SEARCH_SIZE if self._query_type == "person_query" else 1,
            )["hits"]["hits"]
            if not self._search_response:
                raise NoMatch
        return self._search_response

    @property
    def matches(self) -> Deque[Person]:
        if not self._matches:
            self._matches = deque(
                Person.from_doc(doc)
                for doc in self.search_response
            )
        return self._matches

    @property
    def match_keys(self) -> Set[str]:
        if not self._match_keys:
            self._match_keys |= self.person == self.composite
            if not self._match_keys:
                raise NoMatch
        return self._match_keys

    @property
    def match_score(self) -> str:
        if not self._match_score:
            n_match_keys = len(self.match_keys)
            date_score = score(self.composite.date.year)
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
            self._composite = self.matches.popleft()
            for person in self.matches:
                self._composite |= person
            _ = self.match_keys
        return self._composite
