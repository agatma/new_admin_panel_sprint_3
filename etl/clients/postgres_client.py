from __future__ import annotations

import contextlib
from typing import Any

import psycopg2
from psycopg2.extensions import connection as pg_coon
from psycopg2.extensions import cursor as pg_cursor
from psycopg2.sql import SQL
from pydantic import PostgresDsn

from clients.base_client import AbstractClient, AbstractClientInterface
from components.backoff import backoff
from components.logger import logger


class PostgresClient(AbstractClient):
    base_exceptions = psycopg2.OperationalError
    _connection = pg_coon

    def __init__(self, dsn: PostgresDsn, *args, **kwargs):
        super().__init__(dsn, *args, **kwargs)

    @property
    def is_connected(self) -> bool:
        return self._connection and not self._connection.closed

    @backoff(exceptions=(base_exceptions,))
    def connect(self) -> None:
        logger.info("Попытка подключения к postgres")
        self._connection = psycopg2.connect(
            dsn=self.dsn, *self.args, **self.kwargs
        )
        logger.info("Успешное подключение к postgres")

    @backoff(exceptions=(base_exceptions,))
    @contextlib.contextmanager
    def cursor(self) -> "PostgresCursor":
        cursor: PostgresCursor = PostgresCursor(self)

        yield cursor

        cursor.close()

    def reconnect(self) -> None:
        super().reconnect()

    @backoff(exceptions=(base_exceptions,))
    def close(self) -> None:
        super().close()


class PostgresCursor(AbstractClientInterface):
    base_exceptions = psycopg2.OperationalError
    _cursor: pg_cursor

    def __init__(self, connection: PostgresClient, *args, **kwargs):
        self._connection = connection
        self.connect(*args, **kwargs)

    @property
    def is_cursor_opened(self) -> bool:
        return self._cursor and not self._cursor.closed

    @property
    def is_connection_opened(self) -> bool:
        return self._connection.is_connected

    @property
    def is_connected(self) -> bool:
        return self.is_connection_opened and self.is_cursor_opened

    @backoff(exceptions=(base_exceptions,))
    def connect(self, *args, **kwargs) -> None:
        # noinspection PyProtectedMember
        self._cursor: pg_cursor = self._connection._connection.cursor(
            *args, **kwargs
        )
        logger.debug("Создан новый курсор для Postgres")

    def reconnect(self) -> None:
        if not self.is_connection_opened:
            logger.debug("Переподключение к postgres")
            self._connection.connect()

        if not self.is_cursor_opened:
            logger.debug("Создание курсора для postgres соединения")
            self.connect()

    def close(self) -> None:
        if self.is_cursor_opened:
            self._cursor.close()
            logger.debug("Postgres cursor закрыт")

    def execute(self, query: str | SQL, *args, **kwargs) -> None:
        self._cursor.execute(query, *args, **kwargs)

    def fetchmany(self, chunk: int) -> list[Any]:
        return self._cursor.fetchmany(size=chunk)
