from contextlib import suppress
from logging import info
from socket import timeout
from typing import (Any,
                    Dict,
                    Iterator,
                    List,
                    Mapping,
                    Sequence,
                    Union)

from elasticsearch.client import Elasticsearch
from elasticsearch.exceptions import (ElasticsearchException,
                                      AuthenticationException,
                                      AuthorizationException)


class ESClient(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self,
                 es_index: str = None,
                 dev: bool = True,
                 **kwargs
                 ):
        """Client for ElasticSearch"""
        from common.env import getenv
        from common.secrets import get_secret
        usr, pwd = get_secret("es")
        if dev:
            self._host = getenv("MX_ELASTIC_IP_DEV")
            if not es_index:
                es_index = "dev_peter.person_data_20190716"
        else:
            self._host = getenv("MX_ELASTIC_IP_PROD")
            if not es_index:
                es_index = "production_realestate.realestate"
        self.es_index = es_index
        self._port = int(getenv("MX_ELASTIC_PORT", 9200))
        hosts = [{"host": self._host, "port": self._port}]
        config = {"http_auth": (usr, pwd), "timeout": 60, "retry_on_timeout": True}
        super().__init__(hosts, **config)
        self.size = kwargs.pop("size", 20)

    def __repr__(self):
        return f"{self.__class__.__name__}(host='{self._host}', port='{self._port}', index='{self.es_index}')"

    def __str__(self):
        return f"http://{self._host}:{self._port}/{self.es_index}/_stats"

    def find(self,
             query: Union[dict, List[dict], Iterator[dict]] = None,
             hits_only: bool = True,
             source_only: bool = False,
             first_only: bool = False,
             *args, **kwargs
             ) -> Union[List[List[dict]], List[dict], Dict[str, Dict[str, Any]]]:
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
        if "index" in kwargs:
            index = kwargs.pop("index")
        else:
            index = self.es_index
        if not query:
            size = kwargs.pop("size", 1)
            return self.search(index=index, size=size, body={}, *args, **kwargs)
        if isinstance(query, dict):
            query = [query]
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            info("Returning hits only if any([source_only, first_only])")
            hits_only = True
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
                except (AuthenticationException, AuthorizationException):
                    pass
                except (ElasticsearchException, OSError, ConnectionError, timeout) as e:
                    raise ElasticsearchException(q) from e
            if size != 0:
                if hits_only:
                    result = result["hits"]["hits"]
                if source_only:
                    result = [doc["_source"] for doc in result]
                if first_only:
                    with suppress(IndexError):
                        result = result[0]
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def geo_distance(self, *,
                     address_id: str = None,
                     location: Union[
                         Sequence[Union[str, float]],
                         Mapping[str, Union[str, float]]] = None,
                     distance: str = None
                     ) -> Sequence[dict]:
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
            result: Dict[str, Any] = self.find(query=query, first_only=True)
            location = (result["geometry"]["latitude"], result["geometry"]["longitude"])

        location = dict(zip(("latitude", "longitude"), location.values())) \
            if isinstance(location, Mapping) else \
            dict(zip(("latitude", "longitude"), location))

        query = {"query": {"bool": {"filter": {
                        "geo_distance": {
                            "distance": distance,
                            "geometry.geoPoint": {
                                "lat": location["latitude"],
                                "lon": location["longitude"]}}}}},
                        "sort": [{
                            "_geo_distance": {
                                "geometry.geoPoint": {
                                    "lat": location["latitude"],
                                    "lon": location["longitude"]},
                                "order": "asc"}}]}

        results = self.findall(query=query)

        if results:
            results = [result["_source"] for result in results]

        return results

    def findall(self,
                query: Dict[Any, Any],
                index: str = None,
                **kwargs,
                ) -> List[Dict[Any, Any]]:
        """Used for elastic search queries that are larger than the max
        window size of 10,000.
        :param query: Dict[Any, Any]
        :param index: str
        :param kwargs: scroll: str
        :return: List[Dict[Any, Any]]
        """

        scroll = kwargs.pop("scroll", "10m")

        if not index:
            index = "dev_realestate.realestate"

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

    def query(self, field: str = None, value: Union[str, int] = None,
              **kwargs) -> Union[List[dict], Dict[str, Union[Any, dict]]]:
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
