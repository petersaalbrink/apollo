"""Connect to Matrixian's Elasticsearch databases."""

from __future__ import annotations

__all__ = (
    "ESClient",
)

from contextlib import suppress
from logging import debug
from typing import Any, Dict, Iterator, List, Union

from elasticsearch.client import Elasticsearch
from elasticsearch.exceptions import (ElasticsearchException,
                                      NotFoundError,
                                      TransportError)
from urllib3.exceptions import HTTPWarning

from ..env import envfile, getenv
from ..exceptions import ESClientError
from ..handlers import tqdm
from ..secrets import get_secret

# Types
Location = Union[
    List[Union[str, float]],
    Dict[str, Union[str, float]]
]
DictAny = Dict[str, Any]
NestedDict = Dict[str, DictAny]
Query = Union[
    NestedDict,
    List[NestedDict]
]
Result = Union[
    List[NestedDict],
    NestedDict,
    DictAny,
]

# Globals
_config = {
    "timeout": 300,
    "retry_on_timeout": True,
    "http_auth": (),
}
_hosts = {
    "address": "MX_ELASTIC_ADDR_IP",
    "cdqc": "MX_ELASTIC_CDQC_IP",
    "dev": "MX_ELASTIC_DEV_IP",
    "prod": "MX_ELASTIC_PROD_IP",
}
_port = int(getenv("MX_ELASTIC_PORT", 9200))

# Constants
RE_OLD = "dev_realestate.realestate"
RE_NEW = "dev_realestate.real_estate"
REAL_ESTATES = {
    RE_OLD,
    RE_NEW,
}


class ESClient(Elasticsearch):
    """Client for Matrixian's Elasticsearch databases.

    ESClient inherits from the official Elasticsearch client. This means
    that all its methods can be used. In addition, some added methods
    will help you in writing better scripts. These include:

    `find`: Perform an Elasticsearch query, and return the hits.
    Like `search`, but better.

    `geo_distance`: Find all real estate objects for a specific location.

    `findall`: Used for Elasticsearch queries that return more than
    10k documents. Returns all results at once.

    `scrollall`: Used for Elasticsearch queries that return more than
    10k documents. Returns an iterator of documents.

    `query`: Perform a simple Elasticsearch query, and return the hits.

    `total`: The total number of documents within the index.

    `count`: Count the number of documents a query will return.

    `distinct_count`: Provide a count of distinct values in a certain field.

    `distinct_values`: Return distinct values in a certain field.
    """

    def __init__(self,
                 es_index: str = None,
                 **kwargs
                 ):
        """Client for Matrixian's Elasticsearch databases.

        Provide an index (database.collection), and based on the name
        the correct server is selected. Hosts are configured using
        environment variables in the format "MX_ELASTIC_*_IP".

        Authentication happens with the "MX_ELASTIC_USR" and
        "MX_ELASTIC_PWD" credentials.

        Possible keyword arguments include:
        `local`: boolean, if True selects localhost (default False)
        `host`: str, only possible value currently "localhost"
        `dev`: boolean, only if no :param es_index: is provided (default True)
        `size`: int, default results size (default 20, max 10000)
        `retry_on_timeout`: boolean (default True)
        """
        local = kwargs.pop("local", False)
        host = kwargs.pop("host", None)
        dev = kwargs.pop("dev", True)
        if local or host == "localhost":
            self._host, self._port = "localhost", "9200"
            del _config["http_auth"]
        else:
            if es_index and not host:
                if es_index.startswith("cdqc"):
                    envv = _hosts["cdqc"]
                elif "production_" in es_index:
                    envv = _hosts["prod"]
                elif "addressvalidation" in es_index:
                    envv = _hosts["address"]
                else:
                    envv = _hosts["dev"]
            elif host:
                envv = _hosts.get(host)
            else:
                if dev:
                    envv = _hosts["dev"]
                else:
                    envv = _hosts["prod"]
            self._host, self._port = getenv(envv), _port
            if not self._host:
                raise ESClientError(f"Make sure a host is configured for variable"
                                    f" name '{envv}' in file '{envfile}'")
            if not _config["http_auth"]:
                _config["http_auth"] = get_secret("MX_ELASTIC")  # noqa
        hosts = [{"host": self._host, "port": self._port}]
        super().__init__(hosts, **_config)
        self.es_index = es_index
        self.size = kwargs.pop("size", 20)
        self.index_exists = kwargs.get("index_exists")
        self.retry_on_timeout = kwargs.pop("retry_on_timeout", True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(host='{self._host}', port='{self._port}', index='{self.es_index}')"

    def __str__(self) -> str:
        return f"http://{self._host}:{self._port}/{self.es_index}/_stats"

    def find(self,
             query: Query = None,
             *args, **kwargs
             ) -> Result:
        """Perform an Elasticsearch query, and return the hits. Like `search`, but better.

        Uses .search() method on class attribute .es_index with size=10_000. Will try again on errors.
        Accepts a single query (dict) or multiple (list[dict]).
        Returns:
            query: dict and
                not hits_only -> dict
                hits_only -> list[dict]
                source_only -> list[dict]
                first_only -> dict
            query: list[dict] and
                not hits_only -> list[dict]
                hits_only -> list[list[dict]]
                source_only -> list[list[dict]]
                first_only -> list[dict]
        """
        hits_only = kwargs.pop("hits_only", True)
        source_only = kwargs.pop("source_only", False)
        first_only = kwargs.pop("first_only", False)
        with_id = kwargs.pop("with_id", False)

        self._check_index_exists()
        if "index" in kwargs:
            index = kwargs.pop("index")
        else:
            index = self.es_index
        if not query:
            size = kwargs.pop("size", 1)
            result = self.search(index=index, size=size, body={}, *args, **kwargs)
            return result
        if isinstance(query, dict):
            query = (query,)
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            debug("Returning hits only if any([source_only, first_only])")
            hits_only = True
        if with_id:
            debug("Returning hits only if with_id is True, with _source flattened")
            hits_only, source_only, first_only = True, False, False
        if "size" in query[0]:
            size = query[0].pop("size")
        else:
            size = kwargs.pop("size", self.size)
        results = []
        for q in query:
            if not q:
                results.append([])
                continue
            while True:
                try:
                    result = self.search(index=self.es_index, size=size, body=q, *args, **kwargs)
                    break
                except (OSError, HTTPWarning, TransportError) as e:
                    if not (self.retry_on_timeout and "timeout" in f"{e}".lower()):
                        raise
                except ElasticsearchException as e:
                    raise ESClientError(q) from e
            if size != 0:
                if hits_only:
                    result = result["hits"]["hits"]
                if with_id:
                    result = [{**doc, **doc.pop("_source")} for doc in result]
                if source_only:
                    result = [doc["_source"] for doc in result]
                if first_only or size == 1:
                    with suppress(IndexError):
                        result = result[0]
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def geo_distance(self, *,
                     address_id: str = None,
                     location: Location = None,
                     distance: str = None
                     ) -> Result:
        """Find all real estate objects within :param distance: of
        :param address_id: or :param location:.

        :param address_id: Address ID in format
            "postalCode houseNumber houseNumberExt"
        :param location: A tuple, list, or dict of
            a latitude-longitude pair.
        :param distance: Distance (in various units) in format "42km".
        :return: list of results that are :param distance: away.

        Example::
            es = ESClient("dev_realestate.real_estate")
            res = es.geo_distance(
                address_id="1071XB 71 B", distance="10m")
            for d in res:
                print(d["address"]["identification"]["addressId"])
        """

        if self.es_index not in REAL_ESTATES:
            raise ESClientError(f"Index should be in {REAL_ESTATES}.")

        if not any((address_id, location)) or all((address_id, location)):
            raise ESClientError("Provide either an address_id or a location")

        if address_id:
            if self.es_index == RE_OLD:
                query = {"query": {"bool": {"must": {
                    "match": {"avmData.locationData.address_id.keyword": address_id}}}}}
                result: dict[str, Any] = self.find(query=query, first_only=True)
                location = (result["geometry"]["latitude"], result["geometry"]["longitude"])
            else:
                query = {"query": {"bool": {"must": {
                    "match": {"address.identification.addressId.keyword": address_id}}}}}
                result: dict[str, Any] = self.find(query=query, first_only=True)
                location = (result["geometry"]["latitude"], result["geometry"]["longitude"])

        try:
            location = dict(zip(("latitude", "longitude"), location.values()))
        except AttributeError:
            location = dict(zip(("latitude", "longitude"), location))

        if self.es_index == RE_OLD:
            query = {
                "query": {
                    "bool": {
                        "filter": {
                            "geo_distance": {
                                "distance": distance,
                                "geometry.geoPoint": {
                                    "lat": location["latitude"],
                                    "lon": location["longitude"]
                                }}}}},
                "sort": [{
                    "_geo_distance": {
                        "geometry.geoPoint": {
                            "lat": location["latitude"],
                            "lon": location["longitude"]},
                        "order": "asc"
                    }}]}
        else:
            query = {
                "query": {
                    "bool": {
                        "filter": {
                            "geo_distance": {
                                "distance": distance,
                                "geometry.geoPoint.coordinates": [
                                    location["longitude"],
                                    location["latitude"],
                                ]}}}},
                "sort": [{
                    "_geo_distance": {
                        "geometry.geoPoint.coordinates": [
                            location["longitude"],
                            location["latitude"],
                        ],
                        "order": "asc"
                    }}]}

        results = self.findall(query=query)

        if results:
            results = [result["_source"] for result in results]

        return results

    def findall(self,
                query: Query,
                index: str = None,
                **kwargs,
                ) -> Result:
        """Used for Elasticsearch queries that return more than 10k documents.
        Returns all results at once.

        :param query: dict[str, Any]
        :param index: str
        :param kwargs: scroll: str
        :return: list[dict[Any, Any]]
        """

        hits_only = kwargs.pop("hits_only", True)
        source_only = kwargs.pop("source_only", False)
        with_id = kwargs.pop("with_id", False)
        if source_only:
            hits_only = True
        if with_id:
            hits_only, source_only = True, False

        scroll = kwargs.pop("scroll", "10m")
        size = kwargs.pop("size", 10_000)

        if not index:
            index = self.es_index

        data = self.search(index=index, scroll=scroll, size=size, body=query)

        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])
        results = data["hits"]["hits"]

        # We scroll over the results until nothing is returned
        while scroll_size > 0:
            data = self.scroll(scroll_id=sid, scroll=scroll)
            results.extend(data["hits"]["hits"])
            sid = data["_scroll_id"]
            scroll_size = len(data["hits"]["hits"])

        if hits_only:
            data = results
        else:
            data["hits"]["hits"] = results
        if with_id:
            data = [{**doc, **doc.pop("_source")} for doc in data]
        if source_only:
            data = [doc["_source"] for doc in data]

        return data

    def scrollall(self,
                  query: Query = None,
                  index: str = None,
                  **kwargs,
                  ) -> Iterator[Result]:
        """Used for Elasticsearch queries that return more than 10k documents.
        Returns an iterator of documents.

        The default behavior of iterating through documents can be changed
        into iterating through chunks of documents by setting `as_chunks=True`.

        Usage::
            es = ESClient()
            q = {"query": {"bool": {"must_not": {"exists": {"field": "id"}}}}}
            data = iter(es.scrollall(query=q))
            for doc in data:
                pass
        """
        hits_only = kwargs.pop("hits_only", True)
        if not hits_only:
            raise ESClientError("Use `.findall()` instead.")
        source_only = kwargs.pop("source_only", False)
        with_id = kwargs.pop("with_id", False)
        if source_only:
            hits_only = True
        if with_id:
            hits_only, source_only = True, False

        field = kwargs.pop("field", None)
        scroll = kwargs.pop("scroll", "10m")
        as_chunks = kwargs.pop("as_chunks", False)
        chunk_size = kwargs.pop("chunk_size", 10_000 if as_chunks else 1)
        if chunk_size > 10_000:
            chunk_size = 10_000
        use_tqdm = kwargs.pop("use_tqdm", False)
        if not index:
            index = self.es_index
        total = self.count(body=query, index=index)
        bar = tqdm(total=total, disable=not use_tqdm)

        def _return(_data):
            if hits_only:
                _data = _data["hits"]["hits"]
            if with_id:
                _data = ({**d, **d.pop("_source")} for d in _data)
            if source_only:
                _data = (d["_source"] for d in _data)
            return _data

        data = self.search(index=index,
                           scroll=scroll,
                           size=chunk_size,
                           _source=field,
                           body=query)
        sid, scroll_size = data["_scroll_id"], len(data["hits"]["hits"])
        if as_chunks:
            yield _return(data)
        else:
            yield from _return(data)

        # We scroll over the results until nothing is returned
        while scroll_size > 0:
            bar.update(scroll_size)
            data = self.scroll(scroll_id=sid, scroll=scroll)
            sid, scroll_size = data["_scroll_id"], len(data["hits"]["hits"])
            if as_chunks:
                yield _return(data)
            else:
                yield from _return(data)

        bar.close()

    def query(self,
              field: str = None,
              value: Any = None,
              **kwargs
              ) -> Result:
        """Perform a simple Elasticsearch query, and return the hits.

        Uses .find() method instead of regular .search()
        Substitute period . for nested fields with underscore _

        Examples:
            from common.connectors import ESClient
            es = ESClient()
            results = es.query(field="lastname", query="Saalbrink")

            # Add multiple search fields:
            results = es.query(lastname="Saalbrink", address_postalCode="1014AK")
            # This results in the query:
            {"query": {"bool": {"must": [{"match": {"lastname": "Saalbrink"}},
                                         {"match": {"address.postalCode": "1014AK"}}]}}}
        """
        find_kwargs = {
            "size": kwargs.pop("size", self.size),
            "sort": kwargs.pop("sort", None),
            "track_scores": kwargs.pop("track_scores", None),
            "hits_only": kwargs.pop("hits_only", True),
            "source_only": kwargs.pop("source_only", False),
            "first_only": kwargs.pop("first_only", False),
            "with_id": kwargs.pop("with_id", False),
        }

        if field and value:
            q = {"query": {"bool": {"must": [{"match": {field: value}}]}}}
            return self.find(q, **find_kwargs)
        elif field or value:
            raise ESClientError("Provide both field and value.")

        args = {}
        for k in kwargs:
            if "_" in k and not k.startswith("_"):
                args[k.replace("_", ".")] = kwargs[k]
            else:
                args[k] = kwargs[k]
        if len(args) == 1:
            q = {"query": {"bool": {"must": {"match": args}}}}
            return self.find(q, **find_kwargs)
        elif len(args) > 1:
            q = {"query": {"bool": {"must": [{"match": {k: v}} for k, v in args.items()]}}}
            return self.find(q, **find_kwargs)
        else:
            return self.find(**find_kwargs)

    def _check_index_exists(self):
        if not self.index_exists:
            if self.index_exists is None:
                self.index_exists = self.indices.exists(self.es_index)
            if self.index_exists is False:
                raise NotFoundError(404, self.es_index)

    @property
    def total(self) -> int:
        """The total number of documents within the index."""
        self._check_index_exists()
        return self.indices.stats(index=self.es_index)["_all"]["total"]["docs"]["count"]

    def count(self,
              body=None,
              index=None,
              doc_type=None,
              find: dict[str, dict] = None,
              **kwargs
              ) -> int:
        """Count the number of documents a query will return."""
        if index is None:
            index = self.es_index
        if find:
            if body:
                raise ESClientError("Provide either `body` or `find`.")
            body = {"query": find}
        count = super().count(body=body, index=index, doc_type=doc_type, **kwargs)
        return count["count"]

    def distinct_count(self,
                       field: str,
                       find: dict[str, dict] = None
                       ) -> int:
        """Provide a count of distinct values in a certain field.

        See:
https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html
        """

        if not find:
            find = {"match_all": {}}

        while True:
            query = {
                "query": find,
                "aggs": {
                    field: {
                        "composite": {
                            "sources": [
                                {field: {"terms": {"field": field}}}
                            ], "size": 10_000
                        }}}}
            try:
                result: dict[str, Any] = self.find(query, size=0)
                break
            except TransportError as e:
                if "fielddata" in f"{e}" and field[-8:] != ".keyword":
                    field = f"{field}.keyword"
                else:
                    raise ESClientError(query) from e

        n_buckets = 0
        while True:
            agg = result["aggregations"][field]
            buckets = agg["buckets"]
            if not buckets:
                break
            n_buckets += len(buckets)
            query["aggs"][field]["composite"]["after"] = agg["after_key"]
            result = self.find(query, size=0)

        return n_buckets

    def distinct_values(self,
                        field: str,
                        find: dict[str, dict] = None
                        ) -> list[Any]:
        """Return distinct values in a certain field.

        See:
https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html
        """

        if not find:
            find = {"match_all": {}}

        while True:
            query = {
                "query": find,
                "aggs": {
                    "q": {
                        "composite": {
                            "sources": [
                                {"q": {"terms": {"field": field}}}
                            ], "size": 10_000
                        }}}}
            try:
                response: dict[str, Any] = self.find(query, size=0)
                break
            except TransportError as e:
                if "fielddata" in f"{e}" and field[-8:] != ".keyword":
                    field = f"{field}.keyword"
                else:
                    raise ESClientError(query) from e

        result = []
        while True:
            agg = response["aggregations"]["q"]
            values = [key["key"]["q"] for key in agg["buckets"]]
            if not values:
                break
            result += values
            query["aggs"]["q"]["composite"]["after"] = agg["after_key"]
            response = self.find(query, size=0)

        return result
