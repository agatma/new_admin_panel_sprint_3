from http import HTTPStatus

from elasticsearch import (AsyncElasticsearch, Elasticsearch, exceptions,
                           helpers)
from pydantic import AnyHttpUrl

from clients.base_client import AbstractClient
from components.backoff import backoff
from components.backoff import reconnect as client_reconnect
from components.logger import logger

PING_MESSAGE = "Подключение к Elasticsearch: {message}"
CONNECTION_FAIL = (
    "Соединение для {client} клиента Elasticsearch не установлено"
)


class ElasticsearchClient(AbstractClient):
    base_exceptions = exceptions.ConnectionError
    _connection = Elasticsearch

    def __init__(self, dsn: AnyHttpUrl, *args, **kwargs):
        super().__init__(dsn, *args, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._connection and self._connection.ping()

    @backoff(
        exceptions=(
            base_exceptions,
            exceptions.TransportError,
        )
    )
    def connect(self) -> None:
        """Клиент ленивый - нужен явный запрос на ping"""
        logger.info("Попытка подключения к Elasticsearch")
        self._connection = Elasticsearch(self.dsn, *self.args, **self.kwargs)
        if not self.is_connected:
            logger.exception(CONNECTION_FAIL.format("синхронного"))
            raise self.base_exceptions(CONNECTION_FAIL.format("синхронного"))
        logger.info(PING_MESSAGE.format(message=self._connection.ping()))

    @backoff(exceptions=(base_exceptions,))
    def close(self) -> None:
        super().close()

    def reconnect(self) -> None:
        super().reconnect()

    @backoff(
        exceptions=(
            base_exceptions,
            exceptions.RequestError,
        )
    )
    @client_reconnect
    def index_exists(self, index: str) -> bool:
        return self._connection.indices.exists(index=index)

    @backoff(
        exceptions=(
            base_exceptions,
            exceptions.SerializationError,
        )
    )
    @client_reconnect
    def index_create(self, index: str, body: dict):
        return self._connection.indices.create(
            index=index,
            body=body,
            ignore=HTTPStatus.BAD_REQUEST.value,
        )

    def create_index_if_not_exists(self, index_name, index_schema) -> None:
        if not self.index_exists(index=index_name):
            response = self.index_create(
                index=index_name,
                body=index_schema,
            )
            logger.debug(
                f"Индекс {index_name} создан. "
                f"Ответ Elasticsearch: {response}"
            )


class ElasticsearchAsyncClient:
    base_exceptions = exceptions.ConnectionError
    _connection = AsyncElasticsearch

    def __init__(self, dsn, *args, **kwargs):
        self.dsn = dsn
        self.args = args
        self.kwargs = kwargs
        self._connection = AsyncElasticsearch(
            self.dsn, *self.args, **self.kwargs
        )

    @property
    async def is_connected(self) -> bool:
        return self._connection and await self._connection.ping()

    async def close(self) -> None:
        if await self.is_connected:
            await self._connection.close()
            logger.info(f"Соединение закрыто для {self.__class__.__name__}")
        self._connection = None

    async def bulk(self, *args, **kwargs) -> tuple:
        """Клиент ленивый - нужен явный запрос на ping"""
        if not await self.is_connected:
            logger.exception(CONNECTION_FAIL.format("асинхронного"))
            raise self.base_exceptions(CONNECTION_FAIL.format("асинхронного"))
        success, errors = await helpers.async_bulk(
            client=self._connection, *args, **kwargs
        )
        return success, set(errors)
