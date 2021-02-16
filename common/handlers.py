"""Module that contains handlers for files, logging, timing, and runtime.

This module contains several functions and classes that are provided as
utilities to help the flow in your programs. You can use them to speed
up the coding process and make sure you never miss a thing (especially
for long-running scripts).

File handlers:

.. py:function: common.handlers.csv_read
   Generate data read from a csv file.
.. py:function: common.handlers.csv_read_from_zip
   Generate data read from a zipped csv file.
.. py:function: common.handlers.csv_write
   Write data to a csv file.
.. py:function: common.handlers.csv_write_to_zip
   Write data to a csv file and archive it.
.. py:function: common.handlers.zip_file
   Write a file to a zip archive.
.. py:class: common.handlers.ZipData
   Class for processing zip archives containing csv data files.

Logging handlers:

.. py:function: common.handlers.get_logger
   Return an advanced Logger, with output to both file and stream.
.. py:class: common.handlers.Log
   Logger class that by default logs with level debug to stderr.

Timing handlers:

.. py:function: common.handlers.tqdm
   Customized tqdm function.
.. py:function: common.handlers.trange
   Customized trange function.
.. py:function: common.handlers.timer
   Decorator for timing a function and logging it (level: info).
.. py:class: common.handlers.Timer
   Timer class for simple timing tasks.
.. py:class: common.handlers.TicToc
   Time code using a class, context manager, or decorator.
.. py:class: common.handlers.FunctionTimer
   Code timing context manager with logging (level: info).

Runtime handlers:

.. py:function: common.handlers.keep_trying
   Keep trying a callable, until optional timeout.
.. py:function: common.handlers.send_email
   Decorator for sending email notification on success/fail.
.. py:function: common.handlers.pip_upgrade
   Upgrade all installed Python packages using pip.
"""

from __future__ import annotations

__all__ = (
    "FunctionTimer",
    "Log",
    "TicToc",
    "Timer",
    "ZipData",
    "assert_never",
    "chunker",
    "csv_read",
    "csv_read_from_zip",
    "csv_write",
    "csv_write_to_zip",
    "get_logger",
    "keep_trying",
    "pip_upgrade",
    "read_json",
    "read_json_line",
    "read_txt",
    "remove_adjacent",
    "send_email",
    "timer",
    "tqdm",
    "trange",
    "zip_file",
)

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
from time import perf_counter, time
from json import load, loads
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterator,
    NoReturn,
    Optional,
    Type,
    Union,
)
from zipfile import ZipFile, ZIP_DEFLATED

from tqdm import tqdm, trange

from .connectors.mx_email import EmailClient, DEFAULT_EMAIL
from .exceptions import DataError, Timeout, TimerError, ZipDataError

_bar_format = "{l_bar: >16}{bar:20}{r_bar}"
tqdm = partial(tqdm, smoothing=0, bar_format=_bar_format)
trange = partial(trange, smoothing=0, bar_format=_bar_format)


def remove_adjacent(sentence: str) -> str:
    """ Remove adjecent words in a string """
    if sentence and isinstance(sentence, str):
        lst = sentence.split()
        return " ".join([elem for i, elem in enumerate(lst) if i == 0 or lst[i - 1] != elem])
    else:
        return sentence


def chunker(lst: list, n) -> Iterator[list]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def csv_write(data: Union[list[dict], dict],
              filename: Union[Path, str],
              **kwargs) -> None:
    """Write data to a csv file.

    The file will be created if it doesn't exist.

    Provide the following arguments:
        :param data: list of dictionaries or single dictionary
        :param filename: path or file name to write to

    Optionally, provide the following keyword arguments:
        encoding: csv file encoding, default "utf-8"
        delimiter: csv file delimiter, default ","
        mode: file write mode, default "w"
        extrasaction: action to be taken for extra keys, default "raise"
        quotechar: csv file quote character, default '"'
    Additional keyword arguments will be passed through to
    `csv.DictWriter`.
    """
    if not data:
        raise DataError("Nothing to do.")
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


def zip_file(
        file_to_zip: Union[Path, str],
        **kwargs,
):
    """Write a file to a zip archive."""
    file_to_zip = Path(file_to_zip)
    if not file_to_zip.exists():
        raise FileNotFoundError(file_to_zip)
    with ZipFile(
            file=file_to_zip.with_suffix(".zip"),
            mode=kwargs.get("mode", "w"),
            compression=kwargs.get("compression", ZIP_DEFLATED),
            compresslevel=kwargs.get("compresslevel", None),
    ) as f:
        f.write(file_to_zip)
    if kwargs.get("remove", False):
        file_to_zip.unlink()


def csv_write_to_zip(
        data: Union[list[dict], dict],
        filename: Union[Path, str],
        **kwargs,
) -> None:
    """Write data to a csv file and archive it.

    The zip file will be created if it doesn't exist.

    Provide the following arguments:
        :param data: list of dictionaries or single dictionary
        :param filename: path or file name to write to
            (can have csv, zip, or no suffix)

    Optionally, provide the following keyword arguments to csv_write_to_zip:
        compression: method for compression, default ZIP_DEFLATED
        compresslevel: level for compression, default None
        mode: file write mode, default "w"
    Optionally, provide the following keyword arguments to csv_write:
        encoding: csv file encoding, default "utf-8"
        delimiter: csv file delimiter, default ","
        mode: file write mode, default "w"
        extrasaction: action to be taken for extra keys, default "raise"
        quotechar: csv file quote character, default '"'
    Additional keyword arguments will be passed through to
    `csv.DictWriter`.
    """
    compression = kwargs.pop("compression", ZIP_DEFLATED)
    compresslevel = kwargs.pop("compresslevel", None)

    filename = Path(filename)
    if filename.suffix == ".csv":
        pass
    elif filename.suffix == ".zip":
        filename = filename.with_suffix(".csv")
    else:
        filename = Path(f"{filename}.csv")

    csv_write(data=data, filename=filename, **kwargs)

    zip_file(
        file_to_zip=filename,
        mode=kwargs.get("mode", "w"),
        compression=compression,
        compresslevel=compresslevel,
        remove=True,
    )


def csv_read(filename: Union[Path, str],
             **kwargs,
             ) -> Iterator[dict]:
    """Generate data read from a csv file.

    Returns rows as dict, with None instead of empty string. If no
    delimiter is specified, tries to find out if the delimiter is
    either a comma or a semicolon.

    Provide the following arguments:
        :param filename: path or file name to read from

    Optionally, provide the following keyword arguments:
        encoding: csv file encoding, default "utf-8"
        delimiter: csv file delimiter, default ","
        sniff: if True, try to sniff the file dialect, default False
    Additional keyword arguments will be passed through to
    `csv.DictReader`.

    :raises FileNotFoundError: if the file does not exist.
    """
    with open(filename, encoding=kwargs.pop("encoding", "utf-8")) as f:

        # Try to sniff the dialect, if wanted
        if kwargs.pop("sniff", False):
            try:
                kwargs["dialect"] = Sniffer().sniff(f.read())
            except Error:
                kwargs["dialect"] = "excel"
            finally:
                f.seek(0)

        # Try to find a commonly used delimiter
        if "delimiter" not in kwargs:
            row = next(DictReader(f, **kwargs))
            if len(row) == 1 and ";" in list(row.keys())[0]:
                kwargs["delimiter"] = ";"
            f.seek(0)

        # Yield the data
        for row in DictReader(f, **kwargs):
            yield {k: v if v else None for k, v in row.items()}


def csv_read_from_zip(
        zipfilename: Union[Path, str],
        csvfilename: Union[Path, str] = None,
        **kwargs,
) -> Iterator[dict]:
    """Generate data read from a zipped csv file.

    Returns rows as dict, with None instead of empty string. If no
    delimiter is specified, tries to find out if the delimiter is
    either a comma or a semicolon.

    Provide the following arguments:
        :param zipfilename: path or file name of zip to read from
        :param csvfilename: path or file name of csv in zip to read from;
            if None, grabs the first available file

    Optionally, provide the following keyword arguments:
        encoding: csv file encoding, default "utf-8"
        delimiter: csv file delimiter, default ","
    Additional keyword arguments will be passed through to
    `csv.DictReader`.

    :raises DataError: if the csv file does not exist in the zip file, or
        if there are no csv files in the zip file.
    :raises FileNotFoundError: if the zip file does not exist.
    """
    zipfile = ZipFile(zipfilename)

    if not csvfilename:
        try:
            csvfilename = next(name for name in zipfile.namelist() if name.endswith(".csv"))
        except StopIteration:
            zipfile.close()
            raise DataError(f"No CSV file found in archive.")

    try:
        csvfile = TextIOWrapper(zipfile.open(csvfilename), encoding=kwargs.pop("encoding", "utf-8"))
    except KeyError:
        zipfile.close()
        raise DataError(f"CSV file {csvfilename} not found in archive.")

    try:

        # Try to find a commonly used delimiter
        if not kwargs.get("delimiter"):
            d = next(DictReader(csvfile, **kwargs))
            if len(d) == 1 and ";" in list(d)[0]:
                kwargs["delimiter"] = ";"
            csvfile.seek(0)

        # Yield the data
        yield from (
            {k: v if v else None for k, v in d.items()}
            for d in DictReader(csvfile, **kwargs)
        )

    finally:
        zipfile.close()
        csvfile.close()


_logging_levels = {
    "notset": logging.NOTSET,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.FATAL,
    "critical": logging.FATAL,
}
_log_format = ("%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:"
               "%(module)s:%(funcName)s:%(lineno)d:%(message)s")
_date_format = "%Y-%m-%d %H:%M:%S"


class Log:
    """Logger class that by default logs with level debug to stderr.

    Example::
        from common.handlers import Log
        Log(
            level="info",
            filename="my.log",
        )
    """

    def __init__(self, level: str = None, filename: str = None):
        """Logger class that by default logs with level debug to stderr."""
        try:
            self.level = _logging_levels.get(level.lower(), logging.DEBUG)
        except AttributeError:
            self.level = logging.DEBUG
        self.kwargs = {
            "level": self.level,
            "format": _log_format,
            "datefmt": _date_format,
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
               filename: Union[Path, str] = None,
               name: str = None,
               **kwargs
               ) -> logging.Logger:
    """Return an advanced Logger, with output to both file and stream.

    The logger can be set with:
        :param level: the level for logging to file, default: debug
        :param filename: the log file, default: sys.argv[0]
        :param name: the name of the logger, default: root

    Optionally, the level to log to stream can be set using:
        stream_level: default: warning

    Example::
        from common.handlers import get_logger
        logger = get_logger(
            level="debug",
            filename="my.log",
            stream_level="info",
        )
        logger.info("This message is shown in both file and stream.")
        logger.debug("This message is shown in the log file only.")
    """

    # get level, name, and filename
    stream_level = _logging_levels.get(
        kwargs.get("stream_level"),
        logging.WARNING
    )
    level = _logging_levels.get(
        f"{level}".lower(),
        logging.DEBUG
    )
    if not name and __name__ not in {"__main__", "common.handlers"}:
        name = __name__
    if not filename:
        filename = f"{name or Path(sys.argv[0]).stem}.log"

    # create logger
    logger = logging.getLogger(name=name)
    logger.setLevel(level=level)

    # create formatter
    formatter = logging.Formatter(
        fmt=_log_format,
        datefmt=_date_format,
    )

    # create file handler which logs even debug messages,
    # add formatter to handler and handler to logger
    fh = logging.FileHandler(filename=filename, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # create console handler with a higher log level,
    # add formatter to handler and handler to logger
    ch = logging.StreamHandler()
    ch.setLevel(stream_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def __make_message(m: str, f: Callable, args, kwargs):
    """Create a message from a function call."""
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
    """Decorator for sending email notification on success/fail.

    Usage::
        @send_email
        def my_func():
            pass

        @send_email(
            to_address=datateam@matrixiangroup.com,
            on_error_only=True,
            )
        def another_func():
            pass

        my_func()
        another_func()
    """
    if not to_address:
        to_address = DEFAULT_EMAIL

    def decorate(f: Callable = None):
        @wraps(f)
        def wrapped(*args, **kwargs):
            nonlocal message
            ec = EmailClient()
            message = __make_message(message, f, args, kwargs)
            try:
                return_value = f(*args, **kwargs)
                if not on_error_only:
                    ec.send_email(
                        to_address=to_address,
                        message=f"Program finished successfully:\n\n{message}"
                    )
                return return_value
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
             ) -> Union[list[OrderedDict],
                        list[list],
                        dict[str, list[OrderedDict]]]:
        """Load (and optionally remove) data from zip archive. If the
        archive contains multiple csv files, they are returned in
        dict[str, list[OrderedDict]] format.

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

    def write(self, replace: tuple[str, str] = ("", "")):
        """Archives and deflates all data files."""
        with ZipFile(self.file_path, "w", compression=8) as zipfile:
            for file, values in self.data.items():
                if self._dicts:
                    zipfile.writestr(file.replace(*replace), "\n".join(
                        values[0].keys() + [",".join(row.values()) for row in values]))
                else:
                    zipfile.writestr(file.replace(*replace), "\n".join([",".join(row) for row in values]))


class Timer:
    """Timer class for simple timing tasks.

    Example::
        from common.handlers import Timer
        t = Timer()
        [i**i for i in range(10000)]
        print(t.end())
    """

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
    """Time code using a class, context manager, or decorator.

    Example::
        from common.handlers import TicToc

        # As class
        t = TicToc()
        t.start()
        [i**i for i in range(10000)]
        t.stop()

        # As context manager
        with TicToc():
            [i**i for i in range(10000)]

        # As decorator
        @TicToc()
        def my_func():
            return [i**i for i in range(10000)]
        my_func()
    """

    timers: ClassVar[dict[str, float]] = dict()
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
    """Code timing context manager with logging (level: info).

    Example::
        from common.handlers import FunctionTimer
        with FunctionTimer():
            [i**i for i in range(10000)]
    """

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


def timer(f):
    """Decorator for timing a function and logging it (level: info).

    Example::
        from common.handlers import timer
        @timer
        def my_func()
            return [i**i for i in range(10000)]
        my_func()
    """

    @wraps(f)
    def timed(*args, **kwargs):
        start_time = perf_counter()
        return_value = f(*args, **kwargs)
        elapsed_time = perf_counter() - start_time
        logging.info("%s:%.8f", f.__name__, elapsed_time)
        return return_value

    return timed


def keep_trying(
        function: Callable,
        *args,
        exceptions: Union[Type[Exception], tuple[Type[Exception], ...]] = None,
        timeout: Union[int, float] = None,
        **kwargs,
) -> Any:
    """Keep trying a callable, until optional timeout.

    :param function: the callable to execute
    :param args: positional arguments to execute the callable with
    :param kwargs: keyword arguments to execute the callable with
    :param exceptions: the exception(s) to suppress
    :param timeout: the number of seconds to keep retrying

    Example::
        from common.handlers import keep_trying

        def function_with_bug(i: int):
            my_list = []
            return my_list[i]

        # Without any arguments:
        keep_trying(function_with_bug, 8)

        # With optional arguments:
        keep_trying(function_with_bug, i=9, exceptions=IndexError, timeout=1)
    """

    if not exceptions:
        exceptions = Exception
    if timeout:
        start = time()

    def eval_cond():
        if timeout:
            return time() < start + timeout
        return True

    while eval_cond():
        try:
            return function(*args, **kwargs)
        except exceptions as e:
            error = e
    else:
        try:
            raise Timeout from error  # noqa
        except NameError:
            raise Timeout


def pip_upgrade():
    """Upgrade all installed Python packages using pip."""
    packages = [
        dist.project_name for dist in pkg_resources.working_set
        if not dist.project_name.startswith("-")
    ]
    run(["pip", "install", "--upgrade", *packages])


def read_txt(
    filename: Union[Path, str],
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs
):
    """Reads any text file per line and yields stripped"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        for line in tqdm(f, disable=False if use_tqdm else True):
            yield line.strip()


def read_json(
    filename: Union[Path, str],
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs
):
    """Reads and loads any JSON file and yields per object in list"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        json_data = load(f)
    for line in tqdm(json_data, disable=False if use_tqdm else True):
        yield line


def read_json_line(
    filename: Union[Path, str],
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs
):
    """Reads and loads any JSON file that is delimited per line and yields per line"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        for line in tqdm(f, disable=False if use_tqdm else True):
            yield loads(line)


def assert_never(x: NoReturn) -> NoReturn:
    """Utility for exhaustiveness checking.

    See https://github.com/python/typing/issues/735
    """
    raise AssertionError(f"Invalid value: {x!r}")
