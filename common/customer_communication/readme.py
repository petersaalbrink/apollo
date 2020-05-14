import os
from pathlib import Path
from datetime import date


class ReadmeBuilder:
    def __init__(self, df, folder, fname, codebook, documentation, coded_input=False, to_zip=True):

        self.data = df
        self.file_name = fname
        self.folder = folder
        self.to_zip = to_zip

        if not coded_input:
            self.client_name = input("Client Name: ")
            self.contact_person = input("Contact person: ")
            self.readme = input("name of readme file: ")
            self.objective = input(
                'give a short description of the objective of the data request and list the requirements')
            self.version = input('Version:')
        else:
            self.client_name = coded_input['client_name']
            self.contact_person = coded_input['contact_person']
            self.readme = coded_input['readme']
            self.objective = coded_input['objective']
            self.version = coded_input['version']

        self.codebook = codebook
        self.product_doc = documentation
        self.num_records = self.data.shape[0]
        self.num_var = self.data.shape[1]
        self.empty_cells = self.data.isnull().sum().sum()
        self.perc_empty_cells = (self.empty_cells / (self.num_records * self.num_var) * 100).round(2)
        self.date_fields = len((self.data.select_dtypes(include='datetime')).columns)
        self.bool_fields = (self.data.apply(lambda x: x.nunique()) == 2).sum()
        self.text_fields = len((self.data.select_dtypes(include='object')).columns)
        self.numeric_fields = len((self.data.select_dtypes(include='number')).columns)
        self.cols = self.fields(self.data)

    def fields(self, data):
        cols = ''
        for col in data.columns:
            if 'unnamed' not in col.lower():
                cols += '- '
                cols += col
                cols += '\n'
        return cols

    def write_file(self):
        with open(Path(__file__).parent / "readme.txt") as file:
            readme = file.read()
        readme = eval(compile(readme, "<fstring_from_file>", "eval"))
        with open(f'{self.readme}.txt', "w") as file:
            file.write(readme)
        self.folder.write(f'{self.readme}.txt')
        if self.to_zip:
            os.remove(f'{self.readme}.txt')


def readme_exe(df, folder, fname, codebook, documentation, coded_input=False, to_zip=True):
    rm_m = ReadmeBuilder(df, folder, fname, codebook, documentation, coded_input, to_zip)
    rm_m.write_file()
