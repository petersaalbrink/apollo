from zipfile import ZipFile
from io import TextIOWrapper
from itertools import islice
from datetime import datetime
from subprocess import check_call
from pathlib import PurePath, Path
from collections import OrderedDict
from csv import DictReader, DictWriter
from typing import Callable, Dict, List, MutableMapping, Tuple, Union


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


class ZipData:
    """Class for processing zip archives containing csv data files."""
    def __init__(self, file_path: Union[PurePath, str], data_as_dicts: bool = True, **kwargs):
        """Create a ZipData class instance.

        Examples:
            path = Path.cwd() / "test.zip"
            zipt = ZipData(path)
            zipt.open(remove=True)
            zipt.transform(function=custom_func)
            zipt.write(replace=("_In", "_Uit"))
        """
        assert file_path.suffix == ".zip"
        self.remove = False
        self.file_path = file_path
        self._dicts = data_as_dicts
        self.data = {}
        self._encoding = kwargs.get("encoding", "utf-8")
        self._delimiter = kwargs.get("delimiter", ";")

    def _open_all(self, n_lines: int = None):
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                if file.endswith(".csv"):
                    with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as csv:
                        csv = DictReader(csv, delimiter=self._delimiter)
                        self.data[file] = [row for row in islice(csv, n_lines)] if self._dicts else \
                            [csv.fieldnames] + [list(row.values()) for row in islice(csv, n_lines)]
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
                        for row in islice(csv, n_lines):
                            yield row if self._dicts else list(row.values())
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
        """
        if remove:
            from sys import platform
            assert "x" in platform
            self.remove = True
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
