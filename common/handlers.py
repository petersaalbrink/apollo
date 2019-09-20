from zipfile import ZipFile
from io import TextIOWrapper
from subprocess import check_call
from pathlib import PurePath, Path
from csv import DictReader, DictWriter
from typing import Callable, List, MutableMapping, Tuple, Union


def csv_write(data: List[dict], filename: Union[PurePath, str],
              encoding: str = "utf-8", delimiter: str = ",", mode: str = "w") -> None:
    """Simple function for writing a list of dictionaries to a csv file."""
    write_header = True if not Path(filename).exists() else False
    with open(filename, mode, encoding=encoding, newline="") as f:
        csv = DictWriter(f, fieldnames=list(data[0].keys()), delimiter=delimiter)
        if write_header:
            csv.writeheader()
        csv.writerows(data)


def csv_read(filename: Union[PurePath, str], encoding: str = "utf-8", delimiter: str = ",") -> MutableMapping:
    """Simple generator for reading from a csv file. Returns rows as OrderedDict."""
    with open(filename, encoding=encoding) as f:
        for row in DictReader(f, delimiter=delimiter):
            yield row


class ZipData:
    """Class for processing zip archives containing csv data files."""
    def __init__(self, file_path: PurePath, data_as_dicts: bool = False, **kwargs):
        """Create a ZipData class instance.

        Examples:
            path = Path.cwd() / "test.zip"
            zipt = ZipData(path)
            zipt.open(remove=True)
            zipt.transform(function=custom_func)
            zipt.write(replace=("_In", "_Uit"))
        """
        assert file_path.suffix == ".zip"
        self.file_path = file_path
        self._dicts = data_as_dicts
        self.data = {}
        self._encoding = kwargs.get("encoding", "utf-8")
        self._delimiter = kwargs.get("delimiter", ";")

    def open(self, remove: bool = False):
        """Load (and optionally remove) data from zip archive."""
        if remove:
            from sys import platform
            assert "x" in platform
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                if file.endswith(".csv"):
                    with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as csv:
                        csv = DictReader(csv, delimiter=self._delimiter)
                        self.data[file] = [row for row in csv] if self._dicts else \
                            [csv.fieldnames] + [list(row.values()) for row in csv]
                    if remove:
                        check_call(["zip", "-d", zipfile.filename, file])
        if len(self.data) == 1:
            self.data = self.data[list(self.data)[0]]

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
