from http import HTTPStatus

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError
from elasticsearch.exceptions import (
    ConnectionTimeout,
    RequestError,
    SerializationError,
    TransportError,
)
from pydantic import AnyHttpUrl

from clients.base_client import AbstractClient
from components.backoff import backoff
from components.backoff import reconnect as client_reconnect
from components.logger import logger

INDEX_CREATED = "Индекс {name} создан. Ответ Elasticsearch: {response}"
PING_MESSAGE = "Подключение к Elasticsearch: {message}"


class ElasticsearchClient(AbstractClient):
    base_exceptions = ElasticConnectionError
    _connection = Elasticsearch

    def __init__(self, dsn: AnyHttpUrl, *args, **kwargs):
        super().__init__(dsn, *args, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._connection and self._connection.ping()

    @backoff(exceptions=(base_exceptions, TransportError, ConnectionTimeout))
    def connect(self) -> None:
        logger.info("Попытка подключения к Elasticsearch")
        self._connection = Elasticsearch(self.dsn, *self.args, **self.kwargs)
        logger.info(PING_MESSAGE.format(message=self._connection.ping()))

    @backoff(exceptions=(base_exceptions,))
    def close(self) -> None:
        super().close()

    def reconnect(self) -> None:
        super().reconnect()

    @backoff(exceptions=(base_exceptions, SerializationError, RequestError))
    def index_exists(self, index: str) -> None:
        return self._connection.indices.exists(index=index)

    @backoff(exceptions=(base_exceptions, SerializationError, RequestError))
    @client_reconnect
    def index_create(self, index: str, body: dict):
        return self._connection.indices.create(
            index=index,
            body=body,
            ignore=HTTPStatus.BAD_REQUEST.value,
        )

    @backoff(exceptions=(base_exceptions, SerializationError, RequestError))
    @client_reconnect
    def bulk(self, *args, **kwargs) -> None:
        helpers.bulk(self._connection, *args, **kwargs)
