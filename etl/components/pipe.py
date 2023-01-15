import asyncio
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pydantic.error_wrappers import ValidationError

from clients.elasticsearch_clients import ElasticsearchAsyncClient
from clients.postgres_client import PostgresClient, PostgresCursor
from components.logger import logger
from components.storage import State
from components.models import ModelETL
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
            params: ModelETL,
            model: type[BaseModel],
            last_modified: datetime,
            pg_conn: PostgresClient,
            storage: State,
    ):
        self._elastic_conn = elastic_conn
        self._query_args: tuple = (last_modified,) * params.amount_query_args
        self.params = params
        self.model = model
        self._pg_conn = pg_conn
        self._Queue = asyncio.Queue(maxsize=1)
        self._storage = storage

    async def extract(self):
        with self._pg_conn.cursor() as cur:
            cur: PostgresCursor
            cur.execute(self.params.query, self._query_args)
            while data := cur.fetchmany(self.params.batch_size):
                await self._Queue.put(self.transform(rows=data))
            await self._Queue.put(None)

    async def load(self) -> None:
        while data := await self._Queue.get():
            start = time.time()
            documents = [
                {
                    "_index": self.params.index_name,
                    "_id": row.id,
                    "_source": row.dict(),
                }
                for row in data
            ]
            success, errors = await self._elastic_conn.bulk(
                actions=documents,
                index=self.params.index_name,
                chunk_size=self.params.batch_size,
            )

            errors = errors.union(
                self._storage.get_state(f"{self.params.index_name}_elastic_errors")
                or set()
            )
            self._storage.set_state(
                f"{self.params.index_name}_elastic_errors", list(errors)
            )
            if errors:
                logger.error(
                    (
                        f"Произошла ошибка при загрузке документов в индекс "
                        f"{self.params.index_name} Elasticsearch. Документы с ошибкой: "
                        f"{list(errors)}. Подробности: etl_status/state.json"
                    )
                )
            duration = round(time.time() - start, 2)
            logger.info(
                f"Загружено в ES {success} объектов индекса "
                f"{self.params.index_name} за {duration} сек."
            )

    def transform(self, rows):
        start = time.time()
        result, errors, success_id, = (
            [],
            {},
            set(),
        )
        for row in rows:
            try:
                result.append(self.model(**row))
                success_id.add(row["id"])
            except ValidationError as err:
                print(row)
                errors[row["id"]] = str(err)
        errors |= (
                self._storage.get_state(f"{self.params.index_name}_pgsql_errors") or {}
        )
        self._storage.set_state(f"{self.params.index_name}_pgsql_errors", errors)
        success = success_id.union(
            self._storage.get_state(f"{self.params.index_name}_pgsql_success")
            or set()
        )
        self._storage.set_state(
            f"{self.params.index_name}_pgsql_success", list(success)
        )
        duration = round(time.time() - start, 2)
        logger.info(
            f"Успешно загружено {len(success)} объектов индекса {self.params.index_name} "
            f"за {duration} сек."
        )
        if errors:
            logger.error(
                (
                    f"Произошла ошибка при валидации объектов индекса "
                    f"{self.params.index_name} из Postgres."
                    f"{list(errors.keys())}. Подробности: etl_status/state.json"
                )
            )
        return result

    @property
    async def tasks(self):
        producer = asyncio.create_task(self.extract())
        consumer = asyncio.create_task(self.load())
        return producer, consumer
