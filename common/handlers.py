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
.. py:function: common.handlers.progress_bar_timer
   Keep track of how long a script is running.

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
    "profile",
    "progress_bar_timer",
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

import logging
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import ContextDecorator
from cProfile import Profile
from csv import DictReader, DictWriter, Error, Sniffer
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial, wraps
from inspect import ismethod
from io import TextIOWrapper
from json import load, loads
from pathlib import Path
from pstats import SortKey, Stats
from subprocess import run
from time import perf_counter, time
from typing import Any, ClassVar, NoReturn, TypeVar
from zipfile import ZIP_DEFLATED, ZipFile

import pkg_resources
from tqdm import tqdm, trange

from .connectors.mx_email import DEFAULT_EMAIL, EmailClient
from .exceptions import DataError, Timeout, TimerError

_bar_format = "{l_bar: >16}{bar:20}{r_bar}"
tqdm = partial(tqdm, smoothing=0, bar_format=_bar_format)
trange = partial(trange, smoothing=0, bar_format=_bar_format)

T = TypeVar("T")


def remove_adjacent(sentence: str) -> str:
    """Remove adjecent words in a string"""
    if sentence and isinstance(sentence, str):
        lst = sentence.split()
        return " ".join(
            [elem for i, elem in enumerate(lst) if i == 0 or lst[i - 1] != elem]
        )
    else:
        return sentence


def chunker(lst: list[T], n: int) -> Iterator[list[T]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def csv_write(
    data: list[dict[str, Any]] | dict[str, Any],
    filename: Path | str,
    **kwargs: str,
) -> None:
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
    fieldnames = list(data[0].keys()) if isinstance(data, list) else list(data.keys())
    not_exists = mode == "w" or (mode == "a" and not Path(filename).exists())

    with open(filename, mode, encoding=encoding, newline="") as f:
        csv = DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=delimiter,
            extrasaction=extrasaction,
            quotechar=quotechar,
            **kwargs,
        )
        if not_exists:
            csv.writeheader()
        if isinstance(data, list):
            csv.writerows(data)
        else:
            csv.writerow(data)


def zip_file(
    file_to_zip: Path | str,
    zip_name: Path | str | None = None,
    **kwargs: Any,
) -> None:
    """Write a file to a zip archive."""
    file_to_zip = Path(file_to_zip)
    if not file_to_zip.exists():
        raise FileNotFoundError(file_to_zip)
    with ZipFile(
        file=zip_name or file_to_zip.with_suffix(".zip"),
        mode=kwargs.get("mode", "w"),
        compression=kwargs.get("compression", ZIP_DEFLATED),
        compresslevel=kwargs.get("compresslevel", None),
    ) as f:
        f.write(file_to_zip, file_to_zip.name)
    if kwargs.get("remove", False):
        file_to_zip.unlink()


def csv_write_to_zip(
    data: list[dict[str, Any]] | dict[str, Any],
    filename: Path | str,
    **kwargs: Any,
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


def csv_read(
    filename: Path | str,
    **kwargs: Any,
) -> Iterator[dict[str, str | None]]:
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
    zipfilename: Path | str,
    csvfilename: Path | str | None = None,
    **kwargs: Any,
) -> Iterator[dict[str, str | None]]:
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
            csvfilename = next(
                name for name in zipfile.namelist() if name.endswith(".csv")
            )
        except StopIteration:
            zipfile.close()
            raise DataError("No CSV file found in archive.")

    try:
        csvfile = TextIOWrapper(
            zipfile.open(f"{csvfilename}"),
            encoding=kwargs.pop("encoding", "utf-8"),
        )
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
_log_format = (
    "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:"
    "%(module)s:%(funcName)s:%(lineno)d:%(message)s"
)
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

    def __init__(
        self,
        level: int | str | None = None,
        filename: str | None = None,
    ):
        """Logger class that by default logs with level debug to stderr."""
        if isinstance(level, str):
            self.level = _logging_levels.get(level.lower(), logging.DEBUG)
        else:
            self.level = logging.DEBUG
        self.kwargs = {
            "level": self.level,
            "format": _log_format,
            "datefmt": _date_format,
        }
        if filename:
            self.kwargs["handlers"] = (
                logging.FileHandler(filename=filename, encoding="utf-8"),
            )
        else:
            self.kwargs["stream"] = sys.stderr
        logging.basicConfig(**self.kwargs)  # type: ignore

    def __repr__(self) -> str:
        kwargs = ", ".join(f"{k}={v}" for k, v in self.kwargs.items())
        return f"Log({kwargs})"

    def __str__(self) -> str:
        return self.__repr__()


def get_logger(
    level: int | str | None = None,
    filename: Path | str | None = None,
    name: str | None = None,
    **kwargs: Any,
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
    stream_level = _logging_levels.get(kwargs.get("stream_level", ""), logging.WARNING)
    level = _logging_levels.get(f"{level}".lower(), logging.DEBUG)
    assert isinstance(level, (int, str))
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


def __make_message(
    m: str | None,
    f: Callable[..., T],
    args: Any,
    kwargs: Any,
) -> str:
    """Create a message from a function call."""
    if not m:
        m = f"{f.__name__}("
        if args:
            if ismethod(f):
                args = args[1:]
            args = [arg for arg in [f"{arg}" for arg in args] if len(arg) <= 1_000]
            m = f"{m}{', '.join(args)}{', ' if kwargs else ''}"
        if kwargs:
            kwargs = [
                kwarg
                for kwarg in [f"{k}={v}" for k, v in kwargs.items()]
                if len(kwarg) <= 1_000
            ]
            m = f"{m}{', '.join(kwargs)}"
        m = f"{m})"
    return m


def send_email(
    function: Callable[..., T] | None = None,
    *,
    to_address: str | Sequence[str] | None = None,
    message: str | None = None,
    on_error_only: bool = False,
) -> Callable[..., T] | Callable[[Callable[..., T]], Callable[..., T]]:
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

    def decorate(f: Callable[..., T]) -> Callable[..., T]:
        @wraps(f)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            nonlocal message
            ec = EmailClient()
            message = __make_message(message, f, args, kwargs)
            assert to_address is not None
            try:
                return_value = f(*args, **kwargs)
                if not on_error_only:
                    ec.send_email(
                        to_address=to_address,
                        message=f"Program finished successfully:\n\n{message}",
                    )
                return return_value
            except Exception:
                ec.send_email(
                    to_address=to_address,
                    message=message,
                    error_message=True,
                )
                raise

        return wrapped

    if function:
        return decorate(function)
    return decorate


class ZipData:
    def __init__(self, *args: Any, **kwargs: Any):
        raise DeprecationWarning("ZipData is deprecated.")


class Timer:
    """Timer class for simple timing tasks.

    Example::
        from common.handlers import Timer
        t = Timer()
        [i**i for i in range(10000)]
        print(t.end())
    """

    def __init__(self) -> None:
        self.t = self.now()

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    def end(self) -> timedelta:
        return self.now() - self.t

    def __str__(self) -> str:
        return f"{self.end()}".split(".")[0]

    def __repr__(self) -> str:
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

    timers: ClassVar[dict[str, float]] = {}
    name: str | None = None
    text: str = "Elapsed time: {:0.4f} seconds"
    logger: Callable[[str], None] | None = print
    _start_time: float | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialization: add timer to dict of timers"""
        if self.name:
            self.timers.setdefault(self.name, 0)

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError("Timer is running. Use .stop() to stop it")

        self._start_time = perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError("Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        if self.logger:
            self.logger(self.text.format(elapsed_time))  # noqa
        if self.name:
            self.timers[self.name] += elapsed_time

        return elapsed_time

    def __enter__(self) -> TicToc:
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

    def __init__(self, name: str | None = None):
        """Initialization: add timer to dict of timers"""
        self.name = name
        self._start_time: float | None = None

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError("Timer is running. Use .stop() to stop it")

        self._start_time = perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError("Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        logging.info("%s:%.8f", self.name, elapsed_time)  # noqa

        return elapsed_time

    def __enter__(self) -> FunctionTimer:
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Stop the context manager timer"""
        self.stop()


def timer(f: Callable[..., T]) -> Callable[..., T]:
    """Decorator for timing a function and logging it (level: info).

    Example::
        from common.handlers import timer
        @timer
        def my_func()
            return [i**i for i in range(10000)]
        my_func()
    """

    @wraps(f)
    def timed(*args: Any, **kwargs: Any) -> T:
        start_time = perf_counter()
        return_value = f(*args, **kwargs)
        elapsed_time = perf_counter() - start_time
        logging.info("%s:%.8f", f.__name__, elapsed_time)
        return return_value

    return timed


def keep_trying(
    function: Callable[..., T],
    *args: Any,
    exceptions: type[Exception] | tuple[type[Exception], ...] | None = None,  # noqa
    timeout: int | float | None = None,
    **kwargs: Any,
) -> T:
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

    def eval_cond() -> bool:
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


def pip_upgrade() -> None:
    """Upgrade all installed Python packages using pip."""
    packages = [
        dist.project_name
        for dist in pkg_resources.working_set
        if not dist.project_name.startswith("-")
    ]
    run(["pip", "install", "--upgrade", *packages])


def read_txt(
    filename: Path | str,
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> Iterator[str]:
    """Reads any text file per line and yields stripped"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        for line in tqdm(f, disable=False if use_tqdm else True):
            yield line.strip()


def read_json(
    filename: Path | str,
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> Iterator[Any]:
    """Reads and loads any JSON file and yields per object in list"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        json_data = load(f)
    yield from tqdm(json_data, disable=False if use_tqdm else True)


def read_json_line(
    filename: Path | str,
    use_tqdm: bool = False,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> Iterator[Any]:
    """Reads and loads any JSON file that is delimited per line and yields per line"""
    with open(filename, "r", encoding=encoding, **kwargs) as f:
        for line in tqdm(f, disable=False if use_tqdm else True):
            yield loads(line)


def assert_never(x: NoReturn) -> NoReturn:
    """Utility for exhaustiveness checking.

    See https://github.com/python/typing/issues/735
    """
    raise AssertionError(f"Invalid value: {x!r}")


def profile(function: Callable[..., T]) -> Callable[..., T]:
    """Decorator for function profiling.

    Stores a file called "profile.pstat" to the current working directory.
    """

    @wraps(function)
    def profiled(*args: Any, **kwargs: Any) -> T:
        pr = Profile(builtins=False, subcalls=False)
        pr.enable()
        return_value = function(*args, **kwargs)
        pr.disable()
        ps = Stats(pr).sort_stats(SortKey.NFL)
        ps.print_stats()
        ps.dump_stats("profile.pstat")
        return return_value

    return profiled


def progress_bar_timer() -> None:
    """Keep track of how long a script is running."""
    from threading import Thread
    from time import sleep

    def progress() -> None:
        bar = tqdm(bar_format="{elapsed}")
        while True:
            sleep(1)
            bar.update(0)

    Thread(target=progress, daemon=True).start()
