from typing import Generator, List, Optional
from http import HTTPStatus
from elasticsearch import (
    ConnectionError,
    ConnectionTimeout,
    Elasticsearch,
    RequestError,
    SerializationError,
    TransportError,
)
from elasticsearch.helpers import bulk

from components.backoff import backoff
from components.config import es_settings
from components.logger import logger
from components.models import FilmWork
from components.schema import MOVIES_INDEX

INDEX_CREATED = "Индекс {name} создан. Ответ Elasticsearch: {response}"
PING_MESSAGE = "Подключение к Elasticsearch: {message}"


class ElasticLoader:
    def __init__(self):
        self.client: Optional[Elasticsearch] = None

    @backoff(exceptions=(ConnectionError, TransportError, ConnectionTimeout))
    def connect(self):
        logger.info("Попытка подключения к Elasticsearch")
        self.client = Elasticsearch(es_settings.elastic_url())
        logger.info(PING_MESSAGE.format(message=self.client.ping()))

    @backoff(exceptions=(RequestError,))
    def create_index(self):
        if not self.client.indices.exists(index=es_settings.index_name):
            response = self.client.indices.create(
                index=es_settings.index_name,
                body=MOVIES_INDEX,
                ignore=HTTPStatus.BAD_REQUEST.value,
            )
            logger.debug(
                INDEX_CREATED.format(
                    name=es_settings.index_name, response=response
                )
            )

    @backoff(exceptions=(SerializationError,))
    def bulk_load_data(
        self, film_data: Generator[List[FilmWork], None, None]
    ) -> None:
        """
        Загрузка данных в Elasticsearch
        :param film_data: объекты модели FilmWork, прошедшие валидацию
        """
        documents = [
            {
                "_index": es_settings.index_name,
                "_id": row.id,
                "_source": row.dict(),
            }
            for row in film_data
        ]
        bulk(
            self.client,
            documents,
            index=es_settings.index_name,
            refresh="wait_for",
        )
