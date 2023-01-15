import asyncio
from contextlib import closing
from time import sleep

import uvloop
from psycopg2.extras import RealDictCursor

from clients.elasticsearch_clients import ElasticsearchClient
from clients.postgres_client import PostgresClient
from components.logger import logger
from components.etl import ETL
from components.config import AppSettings

ERROR_MESSAGE = "ETL процесс остановлен. Произошла ошибка: {error}."


async def main(settings: AppSettings):
    while True:
        try:
            with closing(
                    PostgresClient(
                        dsn=settings.pg_settings.pg_dsn, cursor_factory=RealDictCursor
                    )
            ) as pg_conn, closing(
                ElasticsearchClient(dsn=settings.es_settings.elastic_dsn)
            ) as elastic_conn:
                etl = ETL(
                    storage_state=settings.storage,
                    elastic_conn=elastic_conn,
                    pg_conn=pg_conn,
                    etl_models=settings.etl_models,
                )
                await etl.load_data_from_postgres_to_elastic()
        except Exception as error:
            logger.exception(ERROR_MESSAGE.format(error=error))
        finally:
            logger.info("Остановка процесса на 5 минут")
            sleep(settings.TIME_INTERVAL)


if __name__ == "__main__":
    settings = AppSettings()
    uvloop.install()
    asyncio.run(main(settings))
