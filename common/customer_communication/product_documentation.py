import os
import requests
import json
from pathlib import Path


class Document:

    def __init__(self, folder, coded_input=False, to_zip=True):
        self.ref = {}
        if not coded_input:
            self.input = input(f'Select product {list(self.ref.keys())}')
        else:
            self.input = coded_input['product']

        self.headers = {'Content-Type': 'application/json', }
        self.auth = ('llaagwater@matrixiangroup.com', 'dIsF8eXhVMUfa1ceeybp1BD2')
        self.link = self.file = self.product = None
        self.folder = folder
        self.to_zip = to_zip

    def get_ref(self):
        with open(Path(__file__).parent / "product_documentation.txt") as file:
            self.ref = json.loads(file.read())
            self.product = self.ref[self.input]

    def download(self):
        url = f'https://matrixiangroup.atlassian.net/wiki/rest/api/content/{self.product["id"]}/child/attachment'
        response = requests.request('GET', url, headers=self.headers, auth=self.auth)

        # Get link
        attachments = json.loads(response.text)['results']
        for attachment in attachments:
            if attachment['title'] == self.product['title']:
                self.link = attachment['_links']['download']

    def save(self):
        self.file = requests.request('GET', f'https://matrixiangroup.atlassian.net/wiki{self.link}',
                                     headers=self.headers, auth=self.auth)
        open(f'{self.product["title"]}', 'wb').write(self.file.content)
        self.folder.write(f'{self.product["title"]}')
        if self.to_zip:
            os.remove(f'{self.product["title"]}')


def documentation_exe(folder, coded_input=False, to_zip=True):
    get = Document(folder, coded_input, to_zip)
    if get.input != 'None':
        get.get_ref()
        get.download()
        get.save()
    return get.product['title']
