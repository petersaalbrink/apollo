import re
from typing import Union
from datetime import datetime
from text_unidecode import unidecode
from common.connectors import ESClient, MongoDB


class NoMatch(Exception):
    pass


class Match:
    """Base Match class"""
    drop_fields = {"source", "previous", "common", "country", "place", "fax", "id", "year",
                   "postalCodeMin", "purposeOfUse", "latitude", "longitude", "title", "state", "dateOfRecord"}
    title_data = {"ac", "ad", "ba", "bc", "bi", "bsc", "d", "de", "dr", "drs", "het", "ing",
                  "ir", "jr", "llb", "llm", "ma", "mr", "msc", "o", "phd", "sr", "t", "van"}

    def __init__(self, strictness: int = 3):
        self.data = self.query = self.result = None
        self._strictness = strictness
        self.es = ESClient()

    def match(self, data: dict, strictness: int = None):
        if strictness:
            self._strictness = strictness
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
        if not self.data.get("lastname"):
            raise NoMatch

        self.query = {"query": {"bool": {"must": [
            {"match": {"lastname": self._clean_lastname(self.data["lastname"])}},
        ]}}} if not fuzzy else {"query": {"bool": {"must": [
            {"match": {"lastname": {"query": self._clean_lastname(self.data["lastname"]), "fuzziness": 1}}},
        ]}}}

        should_list = []
        if self.data.get("initials"):
            should_list.append({"match": {"initials": self._clean_initials(self.data["initials"])}})
        if self.data.get("gender"):
            should_list.append({"match": {"gender": self._clean_gender(self.data["gender"])}})
        if self.data.get("date_of_birth"):
            should_list.append({"match": {"birth.date": self._clean_dob(self.data["date_of_birth"])}})
        if self.data.get("postalCode"):
            should_list.append({"match": {"address.current.postalCode": self.data["postalCode"]}})
        if self.data.get("telephone") and self.data["telephone"] != "0":
            phone_number, phone_type = self._clean_phone(self.data["telephone"])
            should_list.append({"match": {f"phoneNumber.{phone_type}": phone_number}})
        if should_list:
            self.query["query"]["bool"]["should"] = should_list
            # noinspection PyTypeChecker
            self.query["query"]["bool"]["minimum_should_match"] = min(strictness, len(should_list))

    def find_match(self):
        self.result = self.es.find(self.query, sort="dateOfRecord:desc")
        if not self.result:
            raise NoMatch
        self.result = self.result[0]["_source"]
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