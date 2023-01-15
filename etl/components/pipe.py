import asyncio
import time
from abc import ABC, abstractmethod
from datetime import datetime

from psycopg2.extras import RealDictRow
from pydantic.error_wrappers import ValidationError

from clients.elasticsearch_clients import ElasticsearchAsyncClient
from clients.postgres_client import PostgresClient, PostgresCursor
from components.models import ModelETL
from components.config import AppSettings
from pydantic import BaseModel


class AbstractETLInterface(ABC):
    @abstractmethod
    def extract(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def load(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def transform(self, *args, **kwargs):
        raise NotImplementedError


class Pipe(AbstractETLInterface):
    def __init__(
            self,
            elastic_conn: ElasticsearchAsyncClient,
            last_modified: datetime,
            model_params: ModelETL,
            pg_conn: PostgresClient,
            settings: AppSettings,
    ):
        self.model_params = model_params
        self.settings = settings
        self._elastic_conn = elastic_conn
        self._query_args: tuple = (last_modified,) * model_params.amount_query_args
        self._Queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._pg_conn = pg_conn

    async def extract(self) -> None:
        with self._pg_conn.cursor() as cur:
            cur: PostgresCursor
            cur.execute(self.model_params.query, self._query_args)
            while data := cur.fetchmany(self.model_params.batch_size):
                data: list[RealDictRow]
                await self._Queue.put(self.transform(rows=data))
            await self._Queue.put(None)

    async def load(self) -> None:
        while data := await self._Queue.get():
            data: list[RealDictRow]
            start = time.time()
            documents = [
                {
                    "_index": self.model_params.index_name,
                    "_id": row.id,
                    "_source": row.dict(),
                }
                for row in data
            ]
            success, errors = await self._elastic_conn.bulk(
                actions=documents,
                index=self.model_params.index_name,
                chunk_size=self.model_params.batch_size,
            )
            self.save_load_results(errors, start, success)

    def transform(self, rows: list[RealDictRow]) -> list[BaseModel]:
        start = time.time()
        result, errors, success_id, = (
            [],
            {},
            set(),
        )
        for row in rows:
            try:
                result.append(self.model_params.model(**row))
                success_id.add(row["id"])
            except ValidationError as err:
                errors[row["id"]] = str(err)
        self.save_validation_results(errors, start, success_id)
        return result

    @property
    async def tasks(self) -> tuple:
        producer = asyncio.create_task(self.extract())
        consumer = asyncio.create_task(self.load())
        return producer, consumer

    def save_load_results(self, errors, start, success):
        errors = errors.union(
            self.settings.storage.get_state(
                f"{self.model_params.index_name}_elastic_errors"
            )
            or set()
        )
        self.settings.storage.set_state(
            f"{self.model_params.index_name}_elastic_errors", list(errors)
        )
        if errors:
            self.settings.logger.error(
                (
                    f"Произошла ошибка при загрузке документов в индекс "
                    f"{self.model_params.index_name} Elasticsearch. Документы с ошибкой: "
                    f"{list(errors)}. Подробности: etl_status/state.json"
                )
            )
        duration = round(time.time() - start, 2)
        self.settings.logger.info(
            f"Загружено в ES {success} объектов индекса "
            f"{self.model_params.index_name} за {duration} сек."
        )

    def save_validation_results(self, errors, start, success_id):
        errors |= (
                self.settings.storage.get_state(f"{self.model_params.index_name}_pgsql_errors")
                or {}
        )
        self.settings.storage.set_state(
            f"{self.model_params.index_name}_pgsql_errors", errors
        )
        success = success_id.union(
            self.settings.storage.get_state(f"{self.model_params.index_name}_pgsql_success")
            or set()
        )
        self.settings.storage.set_state(
            f"{self.model_params.index_name}_pgsql_success", list(success)
        )
        duration = round(time.time() - start, 2)
        self.settings.logger.info(
            f"Успешно загружено {len(success)} объектов индекса {self.model_params.index_name} "
            f"за {duration} сек."
        )
        if errors:
            self.settings.logger.error(
                (
                    f"Произошла ошибка при валидации объектов индекса "
                    f"{self.model_params.index_name} из Postgres."
                    f"{list(errors.keys())}. Подробности: etl_status/state.json"
                )
            )
