import asyncio
from datetime import datetime

from clients.elasticsearch_clients import (
    ElasticsearchAsyncClient,
    ElasticsearchClient,
)
from clients.postgres_client import PostgresClient
from components.pipe import Pipe
from components.storage import State
from components.models import ModelETL


class ETL:
    def __init__(self, storage_state, elastic_conn, pg_conn, etl_models):
        self.storage_state: State = storage_state
        self.elastic_conn: ElasticsearchClient = elastic_conn
        self.pg_conn: PostgresClient = pg_conn
        self.etl_models: list[ModelETL] = etl_models

    async def load_data_from_postgres_to_elastic(self):
        last_modified = (
                self.storage_state.get_state("modified") or datetime.min
        )
        async_elastic_conn = ElasticsearchAsyncClient(
            dsn=self.elastic_conn.dsn
        )
        tasks = []
        for model_params in self.etl_models:
            self.elastic_conn.create_index_if_not_exists(
                index_name=model_params.index_name,
                index_schema=model_params.index_schema
            )
            etl_pipe = Pipe(
                elastic_conn=async_elastic_conn,
                params=model_params,
                model=model_params.model,
                last_modified=last_modified,
                pg_conn=self.pg_conn,
                storage=self.storage_state,
            )
            tasks.extend([etl_pipe.load(), etl_pipe.extract()])
        await asyncio.gather(
            *tasks,
            return_exceptions=False,
        )
        await async_elastic_conn.close()
        self.storage_state.set_state("modified", datetime.now().isoformat())
