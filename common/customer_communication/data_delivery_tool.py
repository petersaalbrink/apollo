import os
from zipfile import ZipFile, ZIP_LZMA
import pandas as pd

# modules
from .readme import readme_exe
from .product_documentation import documentation_exe
from .codebook import codebook_exe


def data_delivery_tool(
        filename: str,
        readme: bool = True,
        codebook: bool = True,
        documentation: bool = False,
        to_zip: bool = True,
        coded_input: dict = None,
        encoding: str = None,
):
    """Create Customer Communication files.

    example coded_input::
        coded_input = {
            'client_name':'Your Client',
            'objective':'This is the goal of the project',
            'version':'1',
            'product':'CDQC',
            'folder_name':'Test'
        }
    """
    if filename[-3:] == 'csv':
        df = pd.read_csv(filename, encoding=encoding, low_memory=False)
    else:
        df = pd.read_excel(filename, low_memory=False)

    # folder
    if not coded_input:
        folder_name = input("Folder name: ")
    else:
        folder_name = coded_input['folder_name']

    with ZipFile(f'{folder_name}.zip', 'w', compression=ZIP_LZMA) as folder:

        # run
        if codebook:
            cb_name = codebook_exe(df, folder, to_zip)
        else:
            cb_name = None

        if documentation:
            doc_name = documentation_exe(folder, coded_input, to_zip)
        else:
            doc_name = None

        if readme:
            readme_exe(df, folder, filename, cb_name, doc_name, coded_input, to_zip)

        # Writing
        folder.write(f'{filename}')

    if not to_zip:
        os.remove(f'{folder_name}.zip')
