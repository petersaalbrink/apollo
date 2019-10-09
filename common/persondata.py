import re
from json import loads
from requests import Session
from requests.adapters import HTTPAdapter
from typing import Union
from datetime import datetime
from time import localtime, sleep
from collections import namedtuple
from text_unidecode import unidecode
from phonenumbers import is_valid_number, parse
from common.parsers import Checks
from common.connectors import ESClient, MongoDB


class NoMatch(Exception):
    pass


class PersonMatch:
    """Base Match class"""
    drop_fields = {"source", "previous", "common", "country", "place", "fax", "id", "year",
                   "postalCodeMin", "purposeOfUse", "latitude", "longitude", "title", "state", "dateOfRecord"}
    title_data = {"ac", "ad", "ba", "bc", "bi", "bsc", "d", "de", "dr", "drs", "het", "ing",
                  "ir", "jr", "llb", "llm", "ma", "mr", "msc", "o", "phd", "sr", "t", "van"}

    def __init__(self, strictness: int = 3, validate_phone: bool = False):
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

    def match(self, data: dict, strictness: int = None):
        if not strictness:
            strictness = self._strictness
        self.data = data
        self.build_query(strictness=strictness)
        self.find_match()
        if self.result:
            return self.result
        else:
            self.build_query(strictness=strictness, fuzzy=True)
            self.find_match()
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

    def find_match(self):
        # Find all matches
        self.results = self.es.find(self.query, source_only=True, sort="dateOfRecord:desc")
        if not self.results:
            raise NoMatch
        if self.validate_phone:
            self._validate_phone()

        # Get the most recent match but update missing data
        self.result = self.results.pop(0)
        self._update_result()

        # Run twice to flatten nested address dicts
        self._flatten_result()
        self._flatten_result()

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
            number = result["phoneNumber"]["number"]
            if number:
                result = self.vn.find_one({"phoneNumber": number}, {"_id": False, "valid": True})
                if result:
                    valid = result["valid"]
                else:
                    valid = loads(self.session.get(
                        f"http://94.168.87.210:4000/call/+31{number}",
                        auth=("datateam", "matrixian")).text)
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
    Data = namedtuple("Data", [
        "postalCode",
        "houseNumber",
        "houseNumberExt",
        "initials",
        "lastname"
    ])
    Score = namedtuple("Score", [
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
    ])
    es = ESClient()
    vn = MongoDB("dev_peter.validated_numbers")
    session = Session()
    session.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=100))

    def __init__(self, data: dict, **kwargs):
        """Class for phone number enrichment.
        Also take a look at this class's find method.

        The find method returns a dictionary with 3 keys:
            number, source, and score.
                source can be N (name) or A (address)
                score can be 1, 2 or 3 (3 is the highest)

        If you use n=2 as argument in .find(), the keys will be:
            mobile, source, and score.

        Examples:
            PhoneNumberFinder({
                "initials": "P",
                "lastname": "Saalbrink",
                "postalCode": "1071XB",
                "houseNumber": "71",
                "houseNumberExt": "B",
            }).find()
            :return: {'number': 649978891, 'source': 'N', 'score': 3}

            PhoneNumberFinder({
                "initials": "P",
                "lastname": "Saalbrink",
                "postalCode": "1071XB",
                "houseNumber": "71",
                "houseNumberExt": "B",
            }).find()
            :return:
                {'mobile': {'number': 649978891, 'source': 'N', 'score': 3},
                'number': {'number': 203345554, 'source': 'N', 'score': 1}}
        """

        self.index = self.data = None
        self.queries = {}
        self.result = {}

        self.respect_hours = kwargs.pop("respect_hours", True)

        # Get phone numbers for data
        self.load(data)

    def __repr__(self):
        return f"PhoneNumberFinder(result={self.result})"

    def __str__(self):
        return f"PhoneNumberFinder(result={self.result})"

    def load(self, data: dict, a: str = "address.current."):
        """Load queries from data. Used keys:
        initials, lastname, postalCode, houseNumber, houseNumberExt"""

        # Load address data from file
        self.data = self.Data(
            data.get("postalCode"),
            int(float(data.get("houseNumber", 0))),
            data.get("houseNumberExt"),
            Checks.str_or_empty(data.get("initials")).replace(".", ""),
            data.get("lastname"))

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

        if self.data.lastname and self.data.initials:
            q = [{"match": {"lastname": {"query": self.data.lastname, "fuzziness": 2}}},
                 {"match": {"initials": {"query": self.data.initials, "fuzziness": 2}}}]
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

        # Before calling our API, do basic (offline) validation
        valid = is_valid_number(parse(phone, "NL"))
        if valid and not phone.startswith("6"):
            # We will only use our API if it's a landline
            result = self.vn.find_one({"phoneNumber": int(phone)}, {"_id": False, "valid": True})
            if result:
                return result["valid"]
            valid = loads(self.session.get(
                f"http://94.168.87.210:4000/call/+31{phone}",
                auth=("datateam", "matrixian")).text)
        return valid

    def extract_number(self, data, records, record, result, source, score, fuzzy, number_types: list = None):
        for number_type in number_types:
            number = record["phoneNumber"][number_type]
            if result is None and number is not None:
                if self.respect_hours:
                    self.sleep_or_continue()
                if self.validate(f"{number}"):
                    result = number
                    source = "N" if record["lastname"] and record["lastname"] in data.lastname else "A"
                    score = self.Score(
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
                    )
        return result, source, score

    def get_result(self, data, response, fuzzy: bool, result=None, source=None, score=None, number_types: list = None):
        if not number_types:
            number_types = ["number", "mobile"]
        # Get the records that were found for this query (or address), and sort them (most recent first)
        records = sorted(response, key=lambda d: d["dateOfRecord"], reverse=True)
        # Loop over the results for a query, from recent to old
        for record in records:
            # Get fixed and mobile number from records (most recent first) and perform validation
            result, source, score = self.extract_number(
                data, records, record, result, source, score, fuzzy, number_types)
            if result:
                break
        return result, source, score

    def calculate_score(self, result_tuple: namedtuple) -> float:
        """Calculate a quality score for the found number."""
        x = 100

        def date_score(var: str, score: float) -> float:
            return (1 - ((datetime.now().year - int(var)) / (x / 2))) * score

        def source_score(var: str, score: float) -> float:
            return (1 - (({"Kadaster_4": 1,
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
                           }[var] - 1) / (x * 2))) * score

        def death_score(var: int, score: float) -> float:
            return score if var is None else .5 * score

        def name_score(var: int, score: float) -> float:
            return score * (1 - ((var - 1) / x))

        def fuzzy_score(var: bool, score: float) -> float:
            return score * .5 if var else score

        def missing_score(result: namedtuple, score: float) -> float:
            if result.dateOfBirth is None:
                score = .975 * score
            if result.gender is None:
                score = .975 * score
            return score

        def number_score(var: int, score: float) -> float:
            return score * (1 - ((var - 1) / x))

        def occurring_score(var: int, score: float) -> float:
            return score * (1 + ((var - 1) / x))

        def moved_score(var: bool, score: float) -> float:
            return 0 if var else score

        def data_score(data: namedtuple, score: float) -> float:
            return 0 if not data.lastname else score

        def full_score(data: namedtuple, result: namedtuple) -> Union[float, None]:
            if result is None:
                return
            score = data_score(data, moved_score(result.moved, occurring_score(result.occurring, number_score(
                result.phoneNumberNumber, missing_score(result, fuzzy_score(result.isFuzzy, name_score(
                    result.lastNameNumber, death_score(result.deceased, source_score(result.source, date_score(
                        result.yearOfRecord, 1))))))))))
            return score

        def categorize(score: float):
            if score is not None:
                if score >= 3/4:
                    score = 3
                elif score >= 2/4:
                    score = 2
                else:
                    score = 1
            return score

        # Calculate score
        return categorize(full_score(self.data, result_tuple))

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

        assert n in {1, 2}, "n=1 will return any number and n=2 will" \
                            " return fixed and mobile; pick either."

        # Get the ES response for each query
        for q in ["initial", "name", "fuzzy", "address", "name_only"]:
            query = self.queries.get(q)
            if query:
                response = self.es.find(query, size=10,
                                        source_only=True,
                                        sort="dateOfRecord:desc")
                if response:
                    if n == 1:
                        result, source, score = self.get_result(
                            self.data, response, q in {"fuzzy", "name_only"})
                        if result:
                            self.result = {
                                "number": result,
                                "source": source,
                                "score": self.calculate_score(score)
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
                                        "score": self.calculate_score(score)
                                    }
                        if self.result.get("number") and self.result.get("mobile"):
                            break

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
