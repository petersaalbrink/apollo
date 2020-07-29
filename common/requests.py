"""Module for making requests, multithreaded!

This module contains some functions that make two things that often
co-occur easier: making requests, and executing some function for an
iterable using multiple threads. Use these functions to speed up your
scripts.

Making requests:

.. py:function: common.requests.request
   Sends a request. Returns :class:`requests.Response` object.
.. py:function: common.requests.get
   Sends a GET request. Returns :class:`requests.Response` object.
.. py:function: common.requests.post
   Sends a POST request. Returns :class:`requests.Response` object.
.. py:function: common.requests.get_session
   Get session with predefined options for requests.
.. py:function: common.requests.get_proxies
   Generator that returns headers with proxies and agents for requests.

Multithreading:

.. py:function: common.requests.thread
   Thread :param data: with :param function: and optionally do :param process:.
.. py:class: common.requests.ThreadSafeIterator
   Takes an iterator/generator and makes it thread-safe
   by serializing call to the `next` method of given iterator/generator.
.. py:function: common.requests.threadsafe
   Decorator that takes a generator function and makes it thread-safe.
"""

from concurrent.futures import ThreadPoolExecutor, wait
from itertools import cycle
from json import loads
from pathlib import Path
from threading import Lock
from typing import (Any,
                    Callable,
                    Iterable,
                    Iterator,
                    List,
                    Optional,
                    Union)

from psutil import net_io_counters
from requests import Session, Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

Executor = ThreadPoolExecutor


class ThreadSafeIterator:
    """Takes an iterator/generator and makes it thread-safe
    by serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, it):
        self.it = it
        self.lock = Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return self.it.__next__()


def threadsafe(f):
    """Decorator that takes a generator function and makes it thread-safe."""
    def decorate(*args, **kwargs):
        return ThreadSafeIterator(f(*args, **kwargs))
    return decorate


@threadsafe
def get_proxies() -> Iterator[dict]:
    """Generator that returns headers with proxies and agents for requests."""

    # Get proxies
    file = Path(__file__).parent / "etc/proxies.txt"
    with open(file) as f:
        proxies = [loads(line.strip().replace("'", '"'))
                   for line in f]

    # Get user agents
    file = Path(__file__).parent / "etc/agents.txt"
    with open(file) as f:
        agents = [line.strip() for line in f]

    # Yield values
    agents = cycle(agents)
    proxies = cycle(proxies)
    while True:
        kwargs = {
            "headers": {
                "User-Agent": next(agents)
            },
            "proxies": next(proxies),
        }
        yield kwargs


def get_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
) -> Session:
    """Get session with predefined options for requests."""
    def hook(response, *args, **kwargs):  # noqa
        if 400 <= response.status_code < 500:
            response.raise_for_status()
    session = session or Session()
    session.hooks["response"] = [hook]
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=100,
        pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_module_data = {}


def request(method: str,
            url: str,
            **kwargs
            ) -> Union[dict, Response]:
    """Sends a request. Returns :class:`requests.Response` object.

    :param method: method for the request.
    :param url: URL for the new :class:`Request` object.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    if kwargs.pop("use_proxies", False):
        try:
            kwargs.update(next(_module_data["get_kwargs"]))
        except KeyError:
            _module_data["get_kwargs"] = get_proxies()
            kwargs.update(next(_module_data["get_kwargs"]))
    text_only = kwargs.pop("text_only", False)
    try:
        response = _module_data["common_session"].request(method, url, **kwargs)
    except KeyError:
        _module_data["common_session"] = get_session()
        response = _module_data["common_session"].request(method, url, **kwargs)
    if text_only:
        return response.json()
    return response


def get(url: str,
        text_only: bool = False,
        use_proxies: bool = False,
        **kwargs
        ) -> Union[dict, Response]:
    """Sends a GET request. Returns :class:`requests.Response` object.

    :param url: URL for the new :class:`Request` object.
    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param use_proxies: Use a random User-Agent and proxy.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    kwargs.update(text_only=text_only, use_proxies=use_proxies)
    kwargs.setdefault("allow_redirects", True)
    return request("GET", url, **kwargs)


def post(url: str,
         text_only: bool = False,
         use_proxies: bool = False,
         **kwargs
         ) -> Union[dict, Response]:
    """Sends a POST request. Returns :class:`requests.Response` object.

    :param url: URL for the new :class:`Request` object.
    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param use_proxies: Use a random User-Agent and proxy.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    kwargs.update(text_only=text_only, use_proxies=use_proxies)
    return request("POST", url, **kwargs)


def thread(function: Callable,
           data: Iterable,
           process: Callable = None,
           **kwargs
           ) -> Optional[List[Any]]:
    """Thread :param data: with :param function: and optionally do :param process:.

    Usage:
    The :param function: Callable must accept only one input argument;
    this is an item of the :param data: Iterable.
    Hence, :param data: must be an Iterable of input values
    for the :param function: Callable.
    The Callable optionally can return Any value.
    Return values will be returned in a list
    (unless a :param process: Callable is specified).
    If you want to use tqdm or another process bar, do so manually
    (e.g., do `bar.update()` inside the :param function: Callable).

    Example:
        from common import get, thread
        thread(
            function=lambda _: get("http://example.org"),
            data=range(2000),
            process=lambda result: print(result.status_code)
        )"""
    process_chunk_size = kwargs.pop("process_chunk_size", 1_000)
    max_workers = kwargs.pop("max_workers", None)
    if process is None:
        with Executor(max_workers=max_workers) as executor:
            # noinspection PyUnresolvedReferences
            return [f.result() for f in
                    wait({executor.submit(function, row) for row in data},
                         return_when="FIRST_EXCEPTION").done]
    else:
        futures = set()
        with Executor(max_workers=max_workers) as executor:
            for row in data:
                futures.add(executor.submit(function, row))
                if len(futures) == process_chunk_size:
                    done, futures = wait(futures, return_when="FIRST_EXCEPTION")
                    _ = [process(f.result()) for f in done]
            done, futures = wait(futures, return_when="FIRST_EXCEPTION")
            if done:
                _ = [process(f.result()) for f in done]


def calculate_bandwith(function, *args, n: int = 100, **kwargs) -> float:
    """Returns the minimal bandwith usage of `function`."""
    def get_bytes():
        stats = net_io_counters()
        return stats.bytes_recv + stats.bytes_sent

    n_bytes = []
    for _ in range(n):
        old_bytes = get_bytes()
        function(*args, **kwargs)
        new_bytes = get_bytes()
        n_bytes.append(new_bytes - old_bytes)
    bandwith = min(n_bytes)
    print(f"Function '{function.__name__}' uses {round(bandwith / 1024, 2)} KB (N={n})")
    return bandwith
