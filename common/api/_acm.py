from datetime import datetime
from functools import partial

from bs4 import BeautifulSoup
from bson.codec_options import CodecOptions
from pendulum import timezone
from requests.exceptions import ProxyError

from ..connectors import MongoDB
from ..requests import get


class ACM:
    def __init__(self):
        self.n_proxy_errors = 0
        self.TZ = timezone("Europe/Amsterdam")
        self.db = MongoDB("cdqc.phonenumbers").with_options(
            codec_options=CodecOptions(
                tz_aware=True,
                tzinfo=self.TZ,
            ))
        headers = {
            "Range": "bytes=0-5504",
        }
        proxies = {
            "http": "nl.proxiware.com:12000",
            "https": "nl.proxiware.com:12000",
        }
        url = "https://www.acm.nl/nl/onderwerpen/telecommunicatie/telefoonnummers/nummers-doorzoeken/resultaat"
        self.NOT_PORTED = "Dit nummer is niet geporteerd. Voor meer informatie zie de nummerreeks."
        self.acm_get = partial(get, url=url, headers=headers, proxies=proxies)
        self.acm_get_no_proxies = partial(get, url=url, headers=headers)

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

        while True:
            try:
                response = self.acm_get(params=params)
                self.n_proxy_errors = 0
                break
            except ProxyError:
                self.n_proxy_errors += 1
                if self.n_proxy_errors == 10:
                    self.acm_get = self.acm_get_no_proxies

        soup = BeautifulSoup(response.content, "lxml")
        result = soup.find("ul", {"class": "nummerresultdetails"})
        items = result.find_all("li")

        try:
            data = {item.find("strong").text: item.find("p").text for item in items}
        except AttributeError:
            if not items[1].text == self.NOT_PORTED:
                raise
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

        if not doc.get("acm_scraped"):

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

        return phone_obj
