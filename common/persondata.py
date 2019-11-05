import re
from json import dumps
from logging import info, debug
from requests import Session, get
from requests.adapters import HTTPAdapter
from typing import Union, MutableMapping, NamedTuple
from datetime import datetime
from time import localtime, sleep
from collections import namedtuple
from text_unidecode import unidecode
from phonenumbers import is_valid_number, parse
from numpy import zeros
from .parsers import Checks
from .handlers import Timer
from .connectors import ESClient, MongoDB


class NoMatch(Exception):
    pass


class PersonMatch:
    """Base Match class"""
    def __init__(self, strictness: int = 3, validate_phone: bool = False):
        self.drop_fields = {"source", "previous", "common", "country", "place", "fax", "id", "year",
                            "postalCodeMin", "purposeOfUse", "latitude", "longitude", "title", "state", "dateOfRecord"}
        self.title_data = {"ac", "ad", "ba", "bc", "bi", "bsc", "d", "de", "dr", "drs", "het", "ing",
                           "ir", "jr", "llb", "llm", "ma", "mr", "msc", "o", "phd", "sr", "t", "van"}
        self.data = {}
        self.result = {}
        self.results = []
        self.query = {"query": {"bool": {}}}
        self._strictness = strictness
        self.validate_phone = validate_phone
        self.es = ESClient()
        self.vn = MongoDB("dev_peter.validated_numbers")
        self.session = Session()
        self.session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
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
            name = unidecode(re.sub(r"[^\sA-Za-z\u00C0-\u017F]", "", re.sub(r"-", " ", name)).strip()) \
                .replace("÷", "o").replace("y", "ij")
            if name != "" and name.split()[-1].lower() in self.title_data:
                name = " ".join(name.split()[:-1])
        return name

    @staticmethod
    def _clean_initials(initials: str) -> str:
        return re.sub(r"[^A-Za-z\u00C0-\u017F]", "", initials.upper())

    @staticmethod
    def _clean_gender(gender: str) -> str:
        return gender.upper().replace("MAN", "M").replace("VROUW", "V") if gender in [
            "Man", "Vrouw", "MAN", "VROUW", "man", "vrouw", "m", "v", "M", "V"] else ""

    @staticmethod
    def _clean_dob(dob: str) -> datetime:
        try:
            return datetime.strptime(dob, "%Y-%m-%d")
        except ValueError:
            return datetime.strptime(dob, "%d/%m/%Y")

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
                            try:
                                valid = self.session.get(
                                    f"{self.url}{number}",
                                    auth=("datateam", "matrixian")).json()
                                break
                            except IOError:
                                self.url = "http://94.168.87.210:4000/call/+31"
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
                    for sub, value in maybe_nested.items():
                        if key in {"birth", "death"} and sub == "date":
                            sub = f"date_of_{key}"
                        if sub not in self.drop_fields:
                            new_result[sub] = value
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
        self.session = Session()
        self.session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))
        self.url = "http://localhost:5000/call/+31"

        self.es = ESClient()
        self.vn = MongoDB("dev_peter.validated_numbers")
        self.local = get('https://api.ipify.org').text == "94.168.87.210"

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
                    try:
                        valid = self.session.get(
                            f"{self.url}{phone}",
                            auth=("datateam", "matrixian")).json()
                        debug("Request: %s", t.end())
                        break
                    except IOError:
                        self.url = "http://94.168.87.210:4000/call/+31"
                        debug("First try: %s", t.end())
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
            def levenshtein(seq1, seq2):
                size_x = len(seq1) + 1
                size_y = len(seq2) + 1
                matrix = zeros((size_x, size_y))
                for _x in range(size_x):
                    matrix[_x, 0] = _x
                for _y in range(size_y):
                    matrix[0, _y] = _y
                for _x in range(1, size_x):
                    for _y in range(1, size_y):
                        if seq1[_x - 1] == seq2[_y - 1]:
                            matrix[_x, _y] = min(
                                matrix[_x - 1, _y] + 1,
                                matrix[_x - 1, _y - 1],
                                matrix[_x, _y - 1] + 1
                            )
                        else:
                            matrix[_x, _y] = min(
                                matrix[_x - 1, _y] + 1,
                                matrix[_x - 1, _y - 1] + 1,
                                matrix[_x, _y - 1] + 1
                            )
                return 1 - (matrix[size_x - 1, size_y - 1]) / max(len(seq1), len(seq2))
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
                if score >= 2/3:
                    score = 3
                elif score >= 1/3:
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
        return set(doc["title"] for doc in MongoDB("dev_peter.names_data").find({"data": "titles"}, {"title": True}))

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
