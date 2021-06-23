from __future__ import annotations

import os
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

from .cdr_log import cdrlog_exe
from .codebook import codebook_exe
from .product_documentation import documentation_exe
from .readme import readme_exe


def data_delivery_tool(
    filename: str,
    readme: bool = True,
    codebook: bool = True,
    documentation: bool = False,
    log_cdr: bool = True,
    to_zip: bool = True,
    coded_input: dict[str, str] | None = None,
    encoding: str | None = None,
    delimiter: str | None = None,
) -> None:
    """Create Customer Communication files.

    example::
        from common.customer_communication import data_delivery_tool as ddt

        coded_input = {
            "client_name": "Your Client",
            "objective": "This is the goal of the project",
            "version": "1",
            "product": "CDQC",
            "folder_name": "Test"
        }

        ddt("some_file.csv", coded_input=coded_input, documentation=True)
    """
    if filename[-3:] == "csv":
        df = pd.read_csv(
            filename,
            encoding=encoding,
            delimiter=delimiter,
            low_memory=False,
        )
    else:
        df = pd.read_excel(filename, low_memory=False)

    # folder
    if not coded_input:
        folder_name = input("Folder name: ")
    else:
        folder_name = coded_input["folder_name"]

    with ZipFile(f"{folder_name}.zip", "a", compression=ZIP_DEFLATED) as folder:

        # run
        cb_name: str | None
        if codebook:
            cb_name = codebook_exe(
                data=df,
                folder=folder,
                to_zip=to_zip,
            )
        else:
            cb_name = None

        doc_name: str | None
        if documentation:
            doc_name = documentation_exe(
                folder=folder,
                coded_input=coded_input,
                to_zip=to_zip,
            )
        else:
            doc_name = None

        if readme:
            readme_exe(
                df=df,
                folder=folder,
                fname=filename,
                codebook=cb_name,
                documentation=doc_name,
                coded_input=coded_input,
                to_zip=to_zip,
            )

        if log_cdr:
            cdrlog_exe(
                filename=filename,
                delimiter=delimiter,
                encoding=encoding,
            )

        # Writing
        folder.write(f"{filename}")

    if not to_zip:
        os.remove(f"{folder_name}.zip")
