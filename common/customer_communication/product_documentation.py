from __future__ import annotations

import json
import os
from pathlib import Path
from zipfile import ZipFile

import requests
from requests import Response

from ..secrets import get_secret


class Document:
    def __init__(
        self,
        folder: ZipFile,
        coded_input: dict[str, str] | None = None,
        to_zip: bool = True,
    ):
        self.coded_input = coded_input
        self.headers = {"Content-Type": "application/json"}
        self.auth = get_secret("MX_CONFLUENCE")
        with open(Path(__file__).parent / "product_documentation.txt") as file:
            self.ref = json.loads(file.read())
        self.link: str | None = None
        self.file: Response | None = None
        self.folder = folder
        self.to_zip = to_zip

    @property
    def product(self) -> dict[str, str]:
        return self.ref[self.input]

    @property
    def input(self) -> str:
        if not self.coded_input:
            return input(f"Select product {list(self.ref.keys())}")
        else:
            return self.coded_input["product"]

    def download(self) -> None:
        url = (
            "https://matrixiangroup.atlassian.net/wiki/rest/api/content/"
            f'{self.product["id"]}/child/attachment'
        )
        response = requests.request("GET", url, headers=self.headers, auth=self.auth)

        # Get link
        attachments = json.loads(response.text)["results"]
        for attachment in attachments:
            if attachment["title"] == self.product["title"]:
                self.link = attachment["_links"]["download"]

    def save(self) -> None:
        self.file = requests.request(
            "GET",
            f"https://matrixiangroup.atlassian.net/wiki{self.link}",
            headers=self.headers,
            auth=self.auth,
        )
        with open(f'{self.product["title"]}', "wb") as f:
            f.write(self.file.content)
        self.folder.write(f'{self.product["title"]}')
        if self.to_zip:
            os.remove(f'{self.product["title"]}')


def documentation_exe(
    folder: ZipFile, coded_input: dict[str, str] | None = None, to_zip: bool = True
) -> str:
    get = Document(folder, coded_input, to_zip)
    if get.input != "None":
        get.download()
        get.save()
    return get.product["title"]
