from datetime import datetime
from typing import Generator, List, Optional

import psycopg2
from psycopg2 import DatabaseError, OperationalError, ProgrammingError
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from psycopg2.extras import RealDictCursor

from components.backoff import backoff
from components.config import etl_settings, pg_settings
from components.logger import logger
from components.models import FilmWork
from components.queries import last_modified_films_query
from components.validators import validate_film_works


class PostgresExtractor:
    def __init__(self):
        self._connection: Optional[_connection] = None
        self._cursor: Optional[_cursor] = None

    @backoff(exceptions=(OperationalError,))
    def connect_to_postgres(self) -> None:
        logger.info("Попытка подключения к PostgreSQL")
        self._connection = psycopg2.connect(
            **pg_settings.dict(), cursor_factory=RealDictCursor
        )
        self._cursor = self._connection.cursor()
        logger.info("Успешное подключение к PostgreSQL")

    @backoff(exceptions=(DatabaseError, ProgrammingError))
    def extract_data(
        self, last_modified: datetime
    ) -> Generator[List[Optional[FilmWork]], None, None]:
        """
        Функция для загрузки данных из PostgreSQL
        :param last_modified: время изменения, с которого начинать загрузку
        :return: список фильмов FilmWork, прошедших валидацию
        """
        self._cursor.execute(last_modified_films_query, (last_modified,) * 3)
        while data := self._cursor.fetchmany(etl_settings.BATCH_SIZE):
            yield validate_film_works(rows=data)

    def close_connection(self):
        self._connection.cursor()
