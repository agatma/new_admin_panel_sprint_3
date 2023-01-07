from datetime import datetime

from _collections_abc import Iterator

from clients.elasticsearch_client import ElasticsearchClient
from clients.postgres_client import PostgresCursor
from components.config import es_settings, etl_settings
from components.logger import logger
from components.models import FilmWork
from components.queries import last_modified_films_query
from components.validators import validate_film_works

INDEX_CREATED = "Индекс {name} создан. Ответ Elasticsearch: {response}"


def bulk_upload(connection, films) -> None:
    documents = [
        {
            "_index": es_settings.index_name,
            "_id": row.id,
            "_source": row.dict(),
        }
        for row in films
    ]
    connection.bulk(
        documents,
        index=es_settings.index_name,
        refresh="wait_for",
    )


def create_index_if_not_exists(connection: ElasticsearchClient, index, body):
    if not connection.index_exists(index=index):
        response = connection.index_create(
            index=index,
            body=body,
        )
        logger.debug(INDEX_CREATED.format(name=index, response=response))


def extract_from_postgres(
    last_modified: datetime, cursor: PostgresCursor
) -> Iterator[list[FilmWork | None]]:
    cursor.execute(last_modified_films_query, (last_modified,) * 3)
    while data := cursor.fetchmany(etl_settings.BATCH_SIZE):
        yield validate_film_works(rows=data)
