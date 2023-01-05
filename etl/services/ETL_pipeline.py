from datetime import datetime

from services.extract_from_pgsql import PostgresExtractor
from services.load_to_ES import ElasticLoader
from components.storage import storage


class ETL:
    def __init__(self):
        self.postgres = PostgresExtractor()
        self.elastic = ElasticLoader()
        self.storage = storage

    def load_data_from_postgres_to_elastic(self):
        last_modified = self.storage.get_state("modified") or datetime.min
        for films in self.postgres.extract_data(last_modified):
            self.elastic.bulk_load_data(films)
        self.storage.set_state("modified", datetime.now().isoformat())
