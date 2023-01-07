from contextlib import closing
from datetime import datetime

from psycopg2.extras import RealDictCursor

from clients.elasticsearch_client import ElasticsearchClient
from clients.postgres_client import PostgresClient, PostgresCursor
from components.config import es_settings, etl_settings, pg_settings
from components.helpers import (
    create_index_if_not_exists,
    extract_from_postgres,
    streaming_bulk_upload,
)
from components.schema import MOVIES_INDEX
from components.storage import storage


class ETL:
    def __init__(self):
        self.storage = storage

    def load_data_from_postgres_to_elastic(self):
        with closing(
            PostgresClient(
                dsn=pg_settings.pg_dsn, cursor_factory=RealDictCursor
            )
        ) as pg_conn, closing(
            ElasticsearchClient(dsn=es_settings.elastic_dsn)
        ) as elastic_conn:
            elastic_conn: ElasticsearchClient
            pg_conn: PostgresClient
            create_index_if_not_exists(
                connection=elastic_conn,
                index=es_settings.index_name,
                body=MOVIES_INDEX,
            )
            with pg_conn.cursor() as cur:
                cur: PostgresCursor
                last_modified = (
                    self.storage.get_state("modified") or datetime.min
                )
                for films in extract_from_postgres(
                    last_modified=last_modified, cursor=cur
                ):
                    streaming_bulk_upload(
                        connection=elastic_conn,
                        data=films,
                        index=es_settings.index_name,
                        chunk_size=etl_settings.BATCH_SIZE,
                        refresh="wait_for",
                    )
                self.storage.set_state("modified", datetime.now().isoformat())
