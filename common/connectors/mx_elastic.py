from contextlib import suppress
from logging import debug
from typing import (Any,
                    Iterator,
                    MutableMapping,
                    Sequence,
                    Union)

from elasticsearch.client import Elasticsearch
from elasticsearch.exceptions import (ElasticsearchException,
                                      NotFoundError,
                                      TransportError)
from urllib3.exceptions import HTTPWarning

from common.handlers import tqdm

# Types
Location = Union[
    Sequence[Union[str, float]],
    MutableMapping[str, Union[str, float]]
]
NestedDict = MutableMapping[str, Any]
Query = Union[
    NestedDict,
    Sequence[NestedDict]
]
Result = Union[
    NestedDict,
    Sequence[NestedDict],
]


class ESClient(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self,
                 es_index: str = None,
                 **kwargs
                 ):
        """Client for ElasticSearch"""
        config = {"timeout": 300, "retry_on_timeout": True}
        if kwargs.pop("local", False) or kwargs.pop("host", None) == "localhost":
            self._host, self._port = "localhost", "9200"
        else:
            from ..env import getenv
            from ..secrets import get_secret
            secret = get_secret("MX_ELASTIC")
            if es_index:
                if "production_" in es_index:
                    envv = "MX_ELASTIC_PROD_IP"
                elif "addressvalidation" in es_index:
                    envv = "MX_ELASTIC_ADDR_IP"
                else:
                    envv = "MX_ELASTIC_DEV_IP"
            else:
                if kwargs.pop("dev", True):
                    envv = "MX_ELASTIC_DEV_IP"
                    es_index = "dev_peter.person_data_20190716"
                else:
                    envv = "MX_ELASTIC_PROD_IP"
                    es_index = "production_realestate.realestate"
            self._host = getenv(envv)
            if not self._host:
                from ..env import envfile
                raise RuntimeError(f"Make sure a host is configured for variable"
                                   f" name '{envv}' in file '{envfile}'")
            self._port = int(getenv("MX_ELASTIC_PORT", 9200))
            config["http_auth"] = secret

        hosts = [{"host": self._host, "port": self._port}]
        super().__init__(hosts, **config)
        self.es_index = es_index
        self.size = kwargs.pop("size", 20)
        self.index_exists = None
        self.retry_on_timeout = kwargs.pop("retry_on_timeout", True)

    def __repr__(self):
        return f"{self.__class__.__name__}(host='{self._host}', port='{self._port}', index='{self.es_index}')"

    def __str__(self):
        return f"http://{self._host}:{self._port}/{self.es_index}/_stats"

    def find(self,
             query: Query = None,
             hits_only: bool = True,
             source_only: bool = False,
             first_only: bool = False,
             with_id: bool = False,
             *args, **kwargs
             ) -> Result:
        """Perform an ElasticSearch query, and return the hits.

        Uses .search() method on class attribute .es_index with size=10_000. Will try again on errors.
        Accepts a single query (dict) or multiple (List[dict]).
        Returns:
            query: dict and
                not hits_only -> dict
                hits_only -> List[dict]
                source_only -> List[dict]
                first_only -> dict
            query: List[dict] and
                not hits_only -> List[dict]
                hits_only -> List[List[dict]]
                source_only -> List[List[dict]]
                first_only -> List[dict]
        """
        self._check_index_exists()
        if "index" in kwargs:
            index = kwargs.pop("index")
        else:
            index = self.es_index
        if not query:
            size = kwargs.pop("size", 1)
            return self.search(index=index, size=size, body={}, *args, **kwargs)
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
                    if self.retry_on_timeout and "timeout" in f"{e}".lower():
                        pass
                    else:
                        raise
                except ElasticsearchException as e:
                    raise ElasticsearchException(q) from e
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
        :return: List of results that are :param distance: away.

        Example:
            es = ESClient("dev_realestate.realestate")
            res = es.geo_distance(
                address_id="1071XB 71 B", distance="10m")
            for d in res:
                print(d["avmData"]["locationData"]["address_id"])
        """

        if not any((address_id, location)) or all((address_id, location)):
            raise ValueError("Provide either an address_id or a location")

        if address_id:
            query = {"query": {"bool": {"must": [
                {"match": {"avmData.locationData.address_id.keyword": address_id}}]}}}
            result: MutableMapping[str, Any] = self.find(query=query, first_only=True)
            location = (result["geometry"]["latitude"], result["geometry"]["longitude"])

        location = dict(zip(("latitude", "longitude"), location.values())) \
            if isinstance(location, MutableMapping) else \
            dict(zip(("latitude", "longitude"), location))

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

        results = self.findall(query=query)

        if results:
            results = [result["_source"] for result in results]

        return results

    def findall(self,
                query: Query,
                index: str = None,
                **kwargs,
                ) -> Result:
        """Used for elastic search queries that are larger than the max
        window size of 10,000. Returns all results at once.
        :param query: MutableMapping[str, Any]
        :param index: str
        :param kwargs: scroll: str
        :return: Sequence[MutableMapping[Any, Any]]
        """

        scroll = kwargs.pop("scroll", "10m")

        if not index:
            index = self.es_index

        data = self.search(index=index, scroll=scroll, size=10_000, body=query)

        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])
        results = data["hits"]["hits"]

        # We scroll over the results until nothing is returned
        while scroll_size > 0:
            data = self.scroll(scroll_id=sid, scroll=scroll)
            results.extend(data["hits"]["hits"])
            sid = data["_scroll_id"]
            scroll_size = len(data["hits"]["hits"])

        return results

    def scrollall(self,
                  query: Query = None,
                  index: str = None,
                  **kwargs,
                  ) -> Iterator[Result]:
        """Used for elastic search queries that are larger than the max
        window size of 10,000. Returns an iterator of documents.

        The default behavior of iterating through documents can be changed
        into iterating through chunks of documents by setting `as_chunks=True`.

        Usage::
            es = ESClient()
            q = {"query": {"bool": {"must_not": {"exists": {"field": "id"}}}}}
            data = iter(es.scrollall(query=q))
            for doc in data:
                pass
        """
        field = kwargs.pop("field", None)
        scroll = kwargs.pop("scroll", "1440m")
        chunk_size = kwargs.pop("chunk_size", 10_000)
        if chunk_size > 10_000:
            chunk_size = 10_000
        as_chunks = kwargs.pop("as_chunks", False)
        hits_only = kwargs.pop("hits_only", True)
        source_only = kwargs.pop("source_only", False)
        use_tqdm = kwargs.pop("use_tqdm", False)
        if not index:
            index = self.es_index
        total = self.count(body=query, index=index)
        bar = tqdm(total=total, disable=not use_tqdm)

        def _return(_data):
            if hits_only:
                _data = _data["hits"]["hits"]
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
        """Perform a simple ElasticSearch query, and return the hits.

        Uses .find() method instead of regular .search()
        Substitute period . for nested fields with underscore _

        Examples:
            from common.classes import ElasticSearch
            es = ElasticSearch()
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
            raise ValueError("Provide both field and value.")

        args = {}
        for k in kwargs:
            if "_" in k and not k.startswith("_"):
                args[k.replace("_", ".")] = kwargs[k]
            else:
                args[k] = kwargs[k]
        if len(args) == 1:
            q = {"query": {"bool": {"must": [{"match": args}]}}}
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
        self._check_index_exists()
        return self.indices.stats(self.es_index)["_all"]["total"]["docs"]["count"]

    def count(self, body=None, index=None, doc_type=None, **kwargs) -> int:
        if index is None:
            index = self.es_index
        count = super().count(body=body, index=index, doc_type=doc_type, **kwargs)
        return count["count"]
