import asyncio
from datetime import datetime

from clients.elasticsearch_clients import (
    ElasticsearchAsyncClient,
    ElasticsearchClient,
)
from clients.postgres_client import PostgresClient
from components.pipe import Pipe
from components.config import AppSettings


class ETL:
    def __init__(
        self,
        elastic_conn: ElasticsearchClient,
        pg_conn: PostgresClient,
        settings: AppSettings,
    ):
        self.elastic_conn = elastic_conn
        self.pg_conn = pg_conn
        self.settings = settings

    async def start_pipeline(self) -> None:
        last_modified: datetime = (
            self.settings.storage.get_state("modified") or datetime.min
        )
        async_elastic_conn: ElasticsearchAsyncClient = (
            ElasticsearchAsyncClient(dsn=self.elastic_conn.dsn)
        )
        tasks = []
        for model_params in self.settings.etl_models:
            self.elastic_conn.create_index_if_not_exists(
                index_name=model_params.index_name,
                index_schema=model_params.index_schema,
            )
            pipe = Pipe(
                elastic_conn=async_elastic_conn,
                last_modified=last_modified,
                model_params=model_params,
                pg_conn=self.pg_conn,
                settings=self.settings,
            )
            tasks.extend([pipe.load(), pipe.extract()])
        await asyncio.gather(
            *tasks,
            return_exceptions=False,
        )
        self.settings.storage.set_state("modified", datetime.now().isoformat())
        await async_elastic_conn.close()
