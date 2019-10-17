from json import loads
from zipfile import ZipFile
from io import TextIOWrapper
from itertools import islice
from datetime import datetime
from subprocess import check_call
from pathlib import PurePath, Path
from collections import OrderedDict
from csv import DictReader, DictWriter
from requests import Session, Response
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Callable, Dict, Iterable, List, MutableMapping, Tuple, Union

session = Session()
session.mount('http://', HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100))


def get(url, text_only: bool = False, **kwargs) -> Union[dict, Response]:
    """Sends a GET request. Returns :class:`Response` object.

    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param url: URL for the new :class:`Request` object.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    return loads(session.get(url, **kwargs).text) if text_only else session.get(url, **kwargs)


def thread(function: Callable, data: Iterable, process: Callable = None):
    """Thread :param data: with :param function: and optionally do :param process:.

    Example:
        from common import get, thread
        thread(
            function=lambda _: get("http://example.org"),
            data=range(2000),
            process=lambda result: print(result.status_code)
        )"""
    if process is None:
        with ThreadPoolExecutor() as executor:
            return [f.result() for f in
                    wait({executor.submit(function, row) for row in data},
                         return_when='FIRST_EXCEPTION').done]
    else:
        futures = set()
        with ThreadPoolExecutor() as executor:
            for row in data:
                futures.add(executor.submit(function, row))
                if len(futures) == 1000:
                    done, futures = wait(futures, return_when='FIRST_EXCEPTION')
                    [process(f.result()) for f in done]
            done, futures = wait(futures, return_when='FIRST_EXCEPTION')
            if done:
                [process(f.result()) for f in done]


def csv_write(data: Union[List[dict], dict], filename: Union[PurePath, str],
              encoding: str = "utf-8", delimiter: str = ",", mode: str = "w") -> None:
    """Simple function for writing a list of dictionaries to a csv file."""
    write_header = True if mode == "w" or mode == "a" and not Path(filename).exists() else False
    if isinstance(data, list):
        with open(filename, mode, encoding=encoding, newline="") as f:
            csv = DictWriter(f, fieldnames=list(data[0].keys()), delimiter=delimiter)
            if write_header:
                csv.writeheader()
            csv.writerows(data)
    elif isinstance(data, dict):
        with open(filename, mode, encoding=encoding, newline="") as f:
            csv = DictWriter(f, fieldnames=list(data.keys()), delimiter=delimiter)
            if write_header:
                csv.writeheader()
            csv.writerow(data)


def csv_read(filename: Union[PurePath, str], encoding: str = "utf-8", delimiter: str = ",") -> MutableMapping:
    """Simple generator for reading from a csv file. Returns rows as OrderedDict."""
    with open(filename, encoding=encoding) as f:
        for row in DictReader(f, delimiter=delimiter):
            yield row


class Log:
    """Simple logger class that, when initiated, by default logs debug
    to stderr."""
    def __init__(self, level: str = None):
        """Simple logger class that, when initiated, by default logs
        debug to stderr."""
        import sys
        import logging
        self.level = {
            "notset": logging.NOTSET,
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARN,
            "warning": logging.WARN,
            "error": logging.ERROR,
            "fatal": logging.FATAL,
            "critical": logging.FATAL,
        }.get(level, logging.DEBUG)
        logging.basicConfig(stream=sys.stderr, level=self.level)

    def __repr__(self):
        return f"Log({self.level})"

    def __str__(self):
        return f"Log({self.level})"


class ZipData:
    """Class for processing zip archives containing csv data files."""
    def __init__(self,
                 file_path: Union[PurePath, str],
                 data_as_dicts: bool = True,
                 assure_columns: bool = False,
                 **kwargs):
        """Create a ZipData class instance.

        Examples:
            path = Path.cwd() / "test.zip"
            zipt = ZipData(path)
            zipt.open(remove=True)
            zipt.transform(function=custom_func)
            zipt.write(replace=("_In", "_Uit"))
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        assert file_path.suffix == ".zip"
        self.remove = False
        self.file_path = file_path
        self._dicts = data_as_dicts
        self.assure_columns = assure_columns
        self.data = {}
        self._encoding = kwargs.get("encoding", "utf-8")
        self._delimiter = kwargs.get("delimiter", ";")
        self.fieldnames = []
        self.columns = set()
        self.count = 0

    def _open_all(self, n_lines: int = None):
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                if file.endswith(".csv"):
                    with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as csv:
                        csv = DictReader(csv, delimiter=self._delimiter)
                        self.fieldnames = csv.fieldnames
                        if self.assure_columns:
                            if self._dicts:
                                self.data[file] = [
                                    {col: row[col] if col in row else None for col in self.columns}
                                    for row in islice(csv, n_lines)]
                            else:
                                self.data[file] = [csv.fieldnames] + [
                                    list({col: row[col] if col in row else None for col in self.columns}.values())
                                    for row in islice(csv, n_lines)]
                        else:
                            if self._dicts:
                                self.data[file] = [row for row in islice(csv, n_lines)]
                            else:
                                self.data[file] = [csv.fieldnames] + [
                                    list(row.values()) for row in islice(csv, n_lines)]
                    if self.remove:
                        check_call(["zip", "-d", zipfile.filename, file])
        if len(self.data) == 1:
            self.data = self.data[list(self.data)[0]]
        return self.data

    def _open_gen(self, n_lines: int = None):
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                if file.endswith(".csv"):
                    with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as csv:
                        csv = DictReader(csv, delimiter=self._delimiter)
                        self.fieldnames = csv.fieldnames
                        if self.assure_columns:
                            if self._dicts:
                                for row in islice(csv, n_lines):
                                    yield {col: row[col] if col in row else None for col in self.columns}
                            else:
                                for row in islice(csv, n_lines):
                                    yield list({col: row[col] if col in row else None for col in self.columns}.values())
                        else:
                            if self._dicts:
                                for row in islice(csv, n_lines):
                                    yield row
                            else:
                                for row in islice(csv, n_lines):
                                    yield list(row.values())
                    if self.remove:
                        check_call(["zip", "-d", zipfile.filename, file])

    def open(self, remove: bool = False, n_lines: int = None, as_generator: bool = False) \
            -> Union[Dict[str, List[OrderedDict]], List[OrderedDict], List[list]]:
        """Load (and optionally remove) data from zip archive. If the
        archive contains multiple csv files, they are returned in
        Dict[str, List[OrderedDict]] format.

        Example:
            zipdata = ZipData("testfile.zip", delimiter=",")
            for row in zipdata.open(as_generator=True):
                row.pop("index")
                csv_write(row, "cleaned_output.csv")

        Example:
            zipdata = ZipData("testfile.zip")
            zipgen = zipdata.open(as_generator=True)
            for row in tqdm(zipgen, total=zipdata.count):
                pass
        """
        # First, store a count of all rows (of all csv's)
        # in the zip file, and store the field names
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as f:
                    self.columns = self.columns.union(set(DictReader(f, delimiter=self._delimiter).fieldnames))
                    self.count += sum(1 for _ in f) - 1

        if remove:
            from sys import platform
            assert "x" in platform
            self.remove = True

        # Load the data
        return self._open_gen(n_lines) if as_generator else self._open_all(n_lines)

    def transform(self, function: Callable, skip_fieldnames: bool = True, *args, **kwargs):
        """Perform a custom function on all data files.

        Optionally, (keyword) arguments are passed through. By default, skips the file header."""
        for file, data in self.data.items():
            self.data[file] = function(data, *args, **kwargs) if self._dicts else \
                function(data[1:] if skip_fieldnames else data, *args, **kwargs)

    def write(self, replace: Tuple[str, str] = ("", "")):
        """Archives and deflates all data files."""
        with ZipFile(self.file_path, "w", compression=8) as zipfile:
            for file, values in self.data.items():
                if self._dicts:
                    zipfile.writestr(file.replace(*replace), "\n".join(
                        values[0].keys() + [",".join(row.values()) for row in values]))
                else:
                    zipfile.writestr(file.replace(*replace), "\n".join([",".join(row) for row in values]))


class Timer:
    def __init__(self):
        self.t = self.now()

    @staticmethod
    def now():
        return datetime.now()

    def end(self):
        return self.now() - self.t

    def __str__(self):
        return f"{self.end()}".split(".")[0]

    def __repr__(self):
        return f"Timer: {self.end()}".split(".")[0]
