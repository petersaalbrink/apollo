from collections import OrderedDict
from contextlib import ContextDecorator
from csv import DictReader, DictWriter, Error, Sniffer
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial, wraps
from io import TextIOWrapper
from inspect import ismethod
from itertools import islice
import logging
from pathlib import Path
import pkg_resources
from subprocess import run
import sys
from time import perf_counter
from typing import (Any,
                    Callable,
                    ClassVar,
                    Dict,
                    Iterable,
                    List,
                    MutableMapping,
                    Optional,
                    Tuple,
                    Union)
from zipfile import ZipFile
from tqdm import tqdm, trange
from .connectors.mx_email import EmailClient
from .exceptions import TimerError, ZipDataError

tqdm = partial(tqdm, smoothing=0, bar_format="{l_bar: >16}{bar:20}{r_bar}")
trange = partial(trange, smoothing=0, bar_format="{l_bar: >16}{bar:20}{r_bar}")


def get_tqdm(iterable: Iterable = None, desc: str = None, name: str = None, **kwargs) -> tqdm:
    return tqdm(iterable=iterable, desc=desc, disable=name != "__main__", **kwargs)


def csv_write(data: Union[List[dict], dict],
              filename: Union[Path, str],
              **kwargs) -> None:
    """Simple function for writing a list of dictionaries to a csv file."""
    encoding: str = kwargs.pop("encoding", "utf-8")
    delimiter: str = kwargs.pop("delimiter", ",")
    mode: str = kwargs.pop("mode", "w")
    extrasaction: str = kwargs.pop("extrasaction", "raise")
    quotechar: str = kwargs.pop("quotechar", '"')

    multiple_rows = isinstance(data, list)
    fieldnames = list(data[0].keys()) if multiple_rows else list(data.keys())
    not_exists = mode == "w" or (mode == "a" and not Path(filename).exists())

    with open(filename, mode, encoding=encoding, newline="") as f:
        csv = DictWriter(f,
                         fieldnames=fieldnames,
                         delimiter=delimiter,
                         extrasaction=extrasaction,
                         quotechar=quotechar,
                         **kwargs)
        if not_exists:
            csv.writeheader()
        if multiple_rows:
            csv.writerows(data)
        else:
            csv.writerow(data)


def csv_read(filename: Union[Path, str],
             **kwargs,
             ) -> MutableMapping:
    """Simple generator for reading from a csv file.
    Returns rows as OrderedDict, with None instead of empty string."""
    with open(filename, encoding=kwargs.pop("encoding", "utf-8")) as f:
        if kwargs.pop("sniff", False):
            try:
                dialect = Sniffer().sniff(f.read())
            except Error:
                dialect = "excel"
            finally:
                f.seek(0)
        else:
            dialect = "excel"
        for row in DictReader(f, dialect=dialect, **kwargs):
            yield {k: v if v else None for k, v in row.items()}


class Log:
    """Simple logger class that, when initiated, by default logs debug
    to stderr."""
    def __init__(self, level: str = None, filename: str = None):
        """Simple logger class that, when initiated, by default logs
        debug to stderr."""
        self.level = {
            "notset": logging.NOTSET,
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARN,
            "warning": logging.WARN,
            "error": logging.ERROR,
            "fatal": logging.FATAL,
            "critical": logging.FATAL,
        }.get(level.lower(), logging.DEBUG)
        self.kwargs = {
            "level": self.level,
            "format": "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:"
                      "%(module)s:%(funcName)s:%(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
        if filename:
            self.kwargs["handlers"] = (logging.FileHandler(filename=filename,
                                                           encoding="utf-8"),)
        else:
            self.kwargs["stream"] = sys.stderr
        logging.basicConfig(**self.kwargs)

    def __repr__(self):
        kwargs = ", ".join(f"{k}={v}" for k, v in self.kwargs.items())
        return f"Log({kwargs})"

    def __str__(self):
        return self.__repr__()


def get_logger(level: str = None,
               filename: str = None,
               name: str = None
               ) -> logging.Logger:
    """Create an advanced Logger that will output to both file and stream.
    The logger can be set with:
        level (default: debug)
        filename (default: sys.argv[0])
        name (default: root)
    """
    # get level, name, and filename
    level = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "fatal": logging.FATAL,
        "critical": logging.FATAL,
    }.get(level.lower() if level else level, logging.DEBUG)
    if not name and __name__ not in {"__main__", "common.handlers"}:
        name = __name__
    if not filename:
        filename = f"{name or Path(sys.argv[0]).stem}.log"
    # create logger
    logger = logging.getLogger(name=name)
    logger.setLevel(level=level)
    # create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:"
            "%(module)s:%(funcName)s:%(lineno)d:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")
    # create file handler which logs even debug messages,
    # add formatter to handler and handler to logger
    fh = logging.FileHandler(filename=filename, encoding="utf-8")
    fh.setLevel(level=level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # create console handler with a higher log level,
    # add formatter to handler and handler to logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def __make_message(m: str, f: Callable, args, kwargs):
    if not m:
        m = f"{f.__name__}("
        if args:
            if ismethod(f):
                args = args[1:]
            args = [arg for arg in
                    [f"{arg}" for arg in args]
                    if len(arg) <= 1_000]
            m = f"{m}{', '.join(args)}{', ' if kwargs else ''}"
        if kwargs:
            kwargs = [kwarg for kwarg in
                      [f'{k}={v}' for k, v in kwargs.items()]
                      if len(kwarg) <= 1_000]
            m = f"{m}{', '.join(kwargs)}"
        m = f"{m})"
    return m


def send_email(function: Callable = None, *,
               to_address: Union[str, list] = None,
               message: str = None,
               on_error_only: bool = False,
               ) -> Callable:
    """Function decorator to send emails!

    Usage::
        @send_email
        def my_func():
            pass

    or::
        def my_func():
            pass
        send_email(my_func)()
    """
    if not to_address:
        to_address = "psaalbrink@matrixiangroup.com"

    def decorate(f: Callable = None):
        @wraps(f)
        def wrapped(*args, **kwargs):
            nonlocal message
            ec = EmailClient()
            message = __make_message(message, f, args, kwargs)
            try:
                f(*args, **kwargs)
                if not on_error_only:
                    ec.send_email(
                        to_address=to_address,
                        message=f"Program finished successfully:\n\n{message}"
                    )
            except Exception:
                ec.send_email(
                    to_address=to_address,
                    message=message,
                    error_message=True
                )
                raise
        return wrapped

    if function:
        return decorate(function)
    return decorate


class ZipData:
    """Class for processing zip archives containing csv data files."""
    def __init__(self,
                 file_path: Union[Path, str],
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
        if file_path.suffix != ".zip":
            raise ZipDataError(f"File '{file_path}' should be a .zip file.")
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
                        self._helper(file, csv, n_lines)
                    if self.remove:
                        run(["zip", "-d", zipfile.filename, file])
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
                        yield from self._gen_helper(csv, n_lines)
                    if self.remove:
                        run(["zip", "-d", zipfile.filename, file])

    def _helper(self, file: str, csv: DictReader, n_lines: Optional[int]):
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

    def _gen_helper(self, csv: DictReader, n_lines: Optional[int]):
        for row in islice(csv, n_lines):
            if self.assure_columns and self._dicts:
                yield {col: row[col] if col in row else None for col in self.columns}
            elif self.assure_columns:
                yield list({col: row[col] if col in row else None for col in self.columns}.values())
            elif self._dicts:
                yield row
            else:
                yield list(row.values())

    def open(self,
             remove: bool = False,
             n_lines: int = None,
             as_generator: bool = False
             ) -> Union[List[OrderedDict],
                        List[list],
                        Dict[str, List[OrderedDict]]]:
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
            self.remove = "x" in platform

        # Load the data
        return self._open_gen(n_lines) if as_generator else self._open_all(n_lines)

    def transform(self, function: Callable, skip_fieldnames: bool = True, *args, **kwargs):
        """Perform a custom function on all data files.

        Optionally, (keyword) arguments are passed through. By default, skips the file header."""
        for file, data in self.data.items():
            if self._dicts:
                self.data[file] = function(data, *args, **kwargs)
            else:
                self.data[file] = function(data[1:] if skip_fieldnames else data, *args, **kwargs)

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


@dataclass
class TicToc(ContextDecorator):
    """Time code using a class, context manager, or decorator."""

    timers: ClassVar[Dict[str, float]] = dict()
    name: Optional[str] = None
    text: str = "Elapsed time: {:0.4f} seconds"
    logger: Optional[Callable[[str], None]] = print
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialization: add timer to dict of timers"""
        if self.name:
            self.timers.setdefault(self.name, 0)

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        if self.logger:
            self.logger(self.text.format(elapsed_time))  # noqa
        if self.name:
            self.timers[self.name] += elapsed_time

        return elapsed_time

    def __enter__(self) -> "TicToc":
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Stop the context manager timer"""
        self.stop()


class FunctionTimer:
    """Code timing context manager."""

    def __init__(self, name: str = None):
        """Initialization: add timer to dict of timers"""
        self.name = name
        self._start_time: Optional[float] = None

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        logging.info("%s:%.8f", self.name, elapsed_time)  # noqa

        return elapsed_time

    def __enter__(self) -> "FunctionTimer":
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Stop the context manager timer"""
        self.stop()


def timer(func):
    def decorate(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            with FunctionTimer(name=f.__name__):
                f(*args, **kwargs)
        return wrapped
    return decorate(func)


def pip_upgrade():
    packages = [dist.project_name for dist in pkg_resources.working_set]
    run(["pip", "install", "--upgrade", *packages])
