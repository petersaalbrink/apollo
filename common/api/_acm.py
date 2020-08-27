from datetime import datetime
from functools import partial

from bs4 import BeautifulSoup
from bson.codec_options import CodecOptions
from pendulum import timezone
from requests.exceptions import ProxyError

from ..connectors import MongoDB
from ..exceptions import PhoneApiError
from ..requests import get_proxies, get_session


class ACM:
    LUMINATI = "01"
    PROXIWARE = "10"
    MAX_REACHED = "U heeft het maximaal aantal verzoeken per dag bereikt."
    NOT_PORTED = "Dit nummer is niet geporteerd. Voor meer informatie zie de nummerreeks."
    URL = "https://www.acm.nl/nl/onderwerpen/telecommunicatie/telefoonnummers/nummers-doorzoeken/resultaat"
    acm_get = acm_get_no_proxies = None

    def __init__(self):
        self.n_max_reached = {
            self.LUMINATI: 0,
            self.PROXIWARE: 0,
        }
        self.n_proxy_errors = {
            self.LUMINATI: 0,
            self.PROXIWARE: 0,
        }
        self.provider = self.LUMINATI
        self.TZ = timezone("Europe/Amsterdam")
        self.db = MongoDB("cdqc.phonenumbers").with_options(
            codec_options=CodecOptions(
                tz_aware=True,
                tzinfo=self.TZ,
            ))
        self.new_session()

    def new_session(self):
        headers = {
            "Range": "bytes=0-5504",
        }
        if self.provider == self.PROXIWARE:
            proxies = {
                "http": "nl.proxiware.com:12000",
                "https": "nl.proxiware.com:12000",
            }
        elif self.provider == self.LUMINATI:
            proxies = get_proxies()["proxies"]
        else:
            raise PhoneApiError(self.provider)
        session = get_session()
        self.acm_get = partial(session.get, url=self.URL, headers=headers, proxies=proxies)
        self.acm_get_no_proxies = partial(session.get, url=self.URL, headers=headers)

    def acm_request(self, number: int) -> dict:

        params = {
            "query": f"0{number}",
            "nrvrij": "-",
            "nrnummerstatus": "-",
            "nrnummervan": "-",
            "nrnummertm": "-",
            "nrbestemming": "-",
            "portering": "1",
        }

        while not (all(n > 1 for n in self.n_proxy_errors.values())
                   or all(n > 1 for n in self.n_max_reached.values())):

            try:
                response = self.acm_get(params=params)

                if self.MAX_REACHED in response.text:
                    self.n_max_reached[self.provider] += 1
                    self.provider = self.provider[::-1]
                    self.new_session()
                else:
                    self.n_max_reached[self.provider] = self.n_proxy_errors[self.provider] = 0
                    break

            except ProxyError:
                self.n_proxy_errors[self.provider] += 1
                self.provider = sorted(self.n_proxy_errors.items(), key=lambda t: t[1])[0][0]
                self.new_session()

        else:
            response = self.acm_get_no_proxies(params=params)
            if self.MAX_REACHED in response.text:
                raise PhoneApiError(self.MAX_REACHED)

        soup = BeautifulSoup(response.content, "lxml")
        result = soup.find("ul", {"class": "nummerresultdetails"})
        items = result.find_all("li")

        try:
            data = {item.find("strong").text: item.find("p").text for item in items}
        except AttributeError as e:
            try:
                if not items[1].text == self.NOT_PORTED:
                    raise PhoneApiError(result.text) from e
            except IndexError as e:
                raise PhoneApiError(result.text) from e
            data = {"Nummerportering": params["query"], "Laatste portering": "", "Huidige aanbieder": ""}

        return data

    def enrich_doc(self, number_data: dict) -> dict:

        acm_data = self.acm_request(number_data["national_number"])

        number_data["current_carrier"] = acm_data["Huidige aanbieder"]
        number_data["date_portation"] = (
            self.TZ.convert(datetime.strptime(acm_data["Laatste portering"], "%d-%m-%Y"))
            if acm_data["Laatste portering"]
            else None
        )

        return number_data

    def find_doc(self, phone_obj):

        doc = self.db.find_one({"national_number": phone_obj.national_number})

        if doc and not doc.get("acm_scraped"):

            doc = self.enrich_doc(doc)

            self.db.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "current_carrier": doc["current_carrier"],
                    "date_portation": doc["date_portation"],
                    "acm_scraped": True,
                }},
            )

        return doc

    def get_acm_data(self, phone_obj):

        doc = self.find_doc(phone_obj)

        if doc:

            phone_obj.country_iso2 = doc["country_iso2"]
            phone_obj.country_iso3 = doc["country_iso3"]
            phone_obj.country_name = doc["country_name"]
            phone_obj.current_carrier = doc["current_carrier"] or doc["original_carrier"]
            phone_obj.date_allocation = doc["date_allocation"]
            phone_obj.date_cooldown = doc["date_cooldown"]
            phone_obj.date_mutation = doc["date_mutation"]
            phone_obj.date_portation = doc["date_portation"]
            phone_obj.number_status = doc["number_status"]
            phone_obj.number_type = doc["number_type"]
            phone_obj.original_carrier = doc["original_carrier"]

        else:

            phone_obj.valid_number = False

        return phone_obj
