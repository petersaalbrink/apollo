from concurrent.futures import ThreadPoolExecutor, wait
from itertools import cycle
from pathlib import Path
from typing import (Callable,
                    Iterable,
                    Iterator,
                    Union)
from requests import Session, Response
from requests.adapters import HTTPAdapter


def get_proxies() -> Iterator[dict]:

    # Get proxies
    proxies = []
    file = Path(__file__).parent / "etc/proxies.txt"
    with open(file) as f:
        for line in f:
            proxy = line.strip().split(":")
            proxy = f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}"
            proxies.append({"https": proxy, "http": proxy})

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


session = Session()
session.mount('http://', HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100))
get_kwargs = get_proxies()


def get(url, text_only: bool = False, **kwargs) -> Union[dict, Response]:
    """Sends a GET request. Returns :class:`Response` object.

    :param text_only: return JSON data from :class:`Response` as dictionary.
    :param url: URL for the new :class:`Request` object.
    :param kwargs: Optional arguments that ``request`` takes.
    """
    kwargs.update(next(get_kwargs))
    return session.get(url, **kwargs).json() if text_only else session.get(url, **kwargs)


def thread(function: Callable,
           data: Iterable,
           process: Callable = None,
           **kwargs):
    """Thread :param data: with :param function: and optionally do :param process:.

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
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # noinspection PyUnresolvedReferences
            return [f.result() for f in
                    wait({executor.submit(function, row) for row in data},
                         return_when='FIRST_EXCEPTION').done]
    else:
        futures = set()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for row in data:
                futures.add(executor.submit(function, row))
                if len(futures) == process_chunk_size:
                    done, futures = wait(futures, return_when='FIRST_EXCEPTION')
                    _ = [process(f.result()) for f in done]
            done, futures = wait(futures, return_when='FIRST_EXCEPTION')
            if done:
                _ = [process(f.result()) for f in done]
