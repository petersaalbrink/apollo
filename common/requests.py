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
   Returns headers with proxies and agent for requests.

Multithreading:

.. py:function: common.requests.thread
   Thread :param data: with :param function: and optionally do :param process:.
.. py:function: common.requests.thread_queue
   Take any iterable :param seq: and execute :param func: on its items in threads.
.. py:class: common.requests.ThreadSafeIterator
   Takes an iterator/generator and makes it thread-safe
   by serializing call to the `next` method of given iterator/generator.
.. py:function: common.requests.threadsafe
   Decorator that takes a generator function and makes it thread-safe.
"""

from __future__ import annotations

__all__ = (
    "Executor",
    "ThreadSafeIterator",
    "calculate_bandwith",
    "download_file",
    "get",
    "get_proxies",
    "get_session",
    "google_sign_url",
    "post",
    "request",
    "thread",
    "thread_queue",
    "threadsafe",
)

import hmac
import json
import socket
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from functools import lru_cache
from hashlib import sha1
from pathlib import Path
from shutil import copyfileobj
from threading import Lock, Thread
from typing import Any, TypeVar
from urllib.parse import parse_qs, urlparse

from psutil import net_io_counters
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import RequestError

Executor = ThreadPoolExecutor
T = TypeVar("T")


class ThreadSafeIterator:
    """Takes an iterator/generator and makes it thread-safe
    by serializing call to the `next` method of given iterator/generator.
    """

    def __init__(self, it: Iterator[T]):
        self.it = it
        self.lock = Lock()

    def __iter__(self) -> ThreadSafeIterator:
        return self

    def __next__(self) -> T:
        with self.lock:
            return self.it.__next__()


def threadsafe(f: Callable[..., Any]) -> Callable[..., ThreadSafeIterator]:
    """Decorator that takes a generator function and makes it thread-safe."""

    def decorate(*args: Any, **kwargs: Any) -> ThreadSafeIterator:
        return ThreadSafeIterator(f(*args, **kwargs))

    return decorate


@lru_cache
def get_proxies() -> dict[str, dict[str, str]]:
    """Returns headers with proxies and agent for requests."""
    proxy_url = "http://nl.smartproxy.com:10000"
    return {
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/84.0.4147.135 Safari/537.36"
        },
        "proxies": {
            "http": proxy_url,
            "https": proxy_url,
        },
    }


def get_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: tuple[int, ...] = (500, 502, 504),
    session: Session | None = None,
) -> Session:
    """Get session with predefined options for requests."""

    def hook(response: Response, *args: Any, **kwargs: Any) -> None:  # noqa
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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@lru_cache
def _session() -> Session:
    return get_session()


def request(
    method: str,
    url: str,
    **kwargs: Any,
) -> dict[str, Any] | Response:
    """Sends a request. Returns :class:`requests.Response` object.

    :param method: method for the request.
    :param url: URL for the new :class:`Request` object.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    if kwargs.pop("use_proxies", False):
        request_kwargs = get_proxies()
        kwargs["proxies"] = request_kwargs["proxies"]
        kwargs.setdefault("headers", request_kwargs["headers"])
    text_only = kwargs.pop("text_only", False)
    response = _session().request(method, url, **kwargs)
    if text_only:
        return response.json()
    return response


def get(
    url: str,
    text_only: bool = False,
    use_proxies: bool = False,
    **kwargs: Any,
) -> dict[str, Any] | Response:
    """Sends a GET request. Returns :class:`requests.Response` object.

    :param url: URL for the new :class:`Request` object.
    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param use_proxies: Use a random User-Agent and proxy.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    kwargs.update(text_only=text_only, use_proxies=use_proxies)
    kwargs.setdefault("allow_redirects", True)
    return request("GET", url, **kwargs)


def post(
    url: str,
    text_only: bool = False,
    use_proxies: bool = False,
    **kwargs: Any,
) -> dict[str, Any] | Response:
    """Sends a POST request. Returns :class:`requests.Response` object.

    :param url: URL for the new :class:`Request` object.
    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param use_proxies: Use a random User-Agent and proxy.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    kwargs.update(text_only=text_only, use_proxies=use_proxies)
    return request("POST", url, **kwargs)


def thread(
    function: Callable[..., T],
    data: Iterable[Any],
    process: Callable[[T], T] | None = None,
    **kwargs: Any,
) -> list[Any]:
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
    futures: set[Future[T]] = set()
    results: list[T] = []

    if process is None:

        def process(x: T) -> T:
            return x

    def wait_and_process() -> None:
        nonlocal futures
        assert process is not None
        done, futures = wait(futures, return_when=FIRST_EXCEPTION)
        results.extend(process(f.result()) for f in done)

    with Executor(max_workers=max_workers) as executor:
        for d in data:
            futures.add(executor.submit(function, d))
            if len(futures) == process_chunk_size:
                wait_and_process()
        wait_and_process()

    return results


def google_sign_url(input_url: str | bytes, secret: str | bytes) -> str:
    if isinstance(input_url, str):
        input_url = input_url.encode()
    if isinstance(secret, str):
        secret = secret.encode()
    url = urlparse(input_url)
    url_to_sign = url.path + b"?" + url.query
    decoded_key = urlsafe_b64decode(secret)
    signature = hmac.new(decoded_key, url_to_sign, sha1)
    encoded_signature = urlsafe_b64encode(signature.digest())
    original_url = url.scheme + b"://" + url.netloc + url.path + b"?" + url.query
    result = original_url + b"&signature=" + encoded_signature
    return result.decode()


def download_file(
    url: str | None = None,
    filepath: Path | str | None = None,
) -> None:
    if not url or not filepath:
        raise RequestError("Error: url or filepath can not be empty.")

    response = get(url, stream=True)
    assert isinstance(response, Response)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            response.raw.decode_content = True
            copyfileobj(response.raw, f)


def calculate_bandwith(
    function: Callable[..., Any],
    *args: Any,
    n: int = 100,
    **kwargs: Any,
) -> float:
    """Returns the minimal bandwith usage of `function`."""

    def get_bytes() -> float:
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


def listener(port: int = 5678) -> Iterable[Any]:
    """Simple HTTP server on port 5678 and generator for POSTed data.

    Listen to incoming data and yield it to do something with it.
    This function expects you to do the async/threading stuff.

    Example of usage in Python:
        for data in listen_or_poll():
            process(data)  # create a thread for this!

    Example of sending data to this listener using curl:
        curl -i localhost:5678 -d "{\"test\": \"ok\"}"
        curl -i localhost:5678?test=ok

    Example of sending data to this listener using Python requests:
        from requests import Session
        data = {"test": "ok"}
        session = Session()
        response = session.get("http://localhost:5678", params=data)
        response = session.post("http://localhost:5678", data=data)
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", port))
        sock.listen()
        while True:
            conn, _ = sock.accept()
            with conn:
                try:
                    headers, content = conn.recv(1024).split(b"\r\n\r\n")
                    header_list = headers.decode().split("\r\n")
                    if "?" in header_list[0]:
                        data = {
                            k: v[0]
                            for k, v in parse_qs(
                                header_list[0].split()[1].strip("/?")
                            ).items()
                        }
                    else:
                        header_dict = {
                            k.title(): v
                            for k, v in (
                                header.split(": ") for header in header_list[1:]
                            )
                        }
                        content_length = int(header_dict["Content-Length"])
                        while content_length > len(content):
                            content += conn.recv(content_length)
                        content_str = content.decode()
                        if "{" in content_str:
                            data = json.loads(content_str)
                        elif "=" in content_str:
                            data = {k: v[0] for k, v in parse_qs(content_str).items()}
                        else:
                            raise ValueError
                    conn.sendall(b"HTTP/1.1 200 OK\r\n\r\n")
                    yield data
                except (json.JSONDecodeError, KeyError, ValueError):
                    conn.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")


def thread_queue(
    func: Callable[[T], Any],
    seq: Callable[[], Iterable[T]] | Iterable[T],
    **kwargs: Any,
) -> None:
    """Take any iterable :param seq: and execute :param func: on its items in threads.

    Example:
        from common.connectors.mx_mongo import MongoDB
        from common.requests import listener, thread_queue

        db = MongoDB("dev_peter.test_data")

        thread_queue(lambda d: db.insert_one(d), listener)
    """
    if not (
        hasattr(func, "__code__")
        and (
            func.__code__.co_argcount == 1  # noqa
            or (
                func.__code__.co_argcount == 2  # noqa
                and func.__code__.co_varnames[0] == "self"  # noqa
            )
        )
    ):
        raise Exception(
            f"'{func.__name__}' should be a function that accepts exactly one argument."
        )

    maxlen = kwargs.pop("maxlen", None)
    futures: deque[Future[Any]] = deque(maxlen=maxlen)
    queue: deque[T] = deque(maxlen=maxlen)
    if callable(seq):
        seq = seq()

    def fill_queue() -> None:
        assert isinstance(seq, Iterable)
        for item in seq:
            queue.append(item)

    def wait_() -> None:
        while futures:
            futures.popleft().result()

    with ThreadPoolExecutor(max_workers=kwargs.pop("max_workers", None)) as executor:
        future = executor.submit(fill_queue)
        while future.running():
            if queue:
                futures.append(executor.submit(func, queue.popleft()))
        future.result()
        thread_ = Thread(target=wait_)
        thread_.start()
        futures.extend(executor.submit(func, item) for item in queue)
        queue.clear()
        thread_.join()
