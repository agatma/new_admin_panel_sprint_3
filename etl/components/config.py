import logging
from pathlib import Path

from pydantic import BaseSettings, Field

from components import indexes, queries
from components.logger import logger
from components.models import FilmWork, Genre, ModelETL, PersonFilms
from components.storage import JsonFileStorage, State


class PostgresConfig(BaseSettings):
    dbname: str = Field(..., env="POSTGRES_DB")
    user: str = Field(..., env="POSTGRES_USER")
    password: str = Field(..., env="POSTGRES_PASSWORD")
    host: str = Field(..., env="POSTGRES_HOST")
    port: int = Field(..., env="POSTGRES_PORT")
    options: str = Field(..., env="POSTGRES_OPTIONS")

    @property
    def pg_dsn(self):
        return (
            f"postgres://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )

    class Config:
        case_sensitive = False
        env_file = ".env"
        env_file_encoding = "utf-8"


class ElasticConfig(BaseSettings):
    host: str = Field("localhost", env="ELASTIC_HOST")
    port: int = Field(9200, env="ELASTIC_PORT")

    @property
    def elastic_dsn(self):
        return f"http://{self.host}:{self.port}/"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class ETLConfig(BaseSettings):
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


"""Настройки моделей ETL"""
ETLFilmModel = ModelETL(
    index_name="movies",
    index_schema=indexes.FILMWORK_INDEX,
    query=queries.last_modified_films_query,
    amount_query_args=3,
    model=FilmWork,
    batch_size=50,
)
ETLGenreModel = ModelETL(
    index_name="genres",
    index_schema=indexes.GENRE_INDEX,
    query=queries.last_modified_genres_query,
    amount_query_args=1,
    model=Genre,
    batch_size=100,
)
ETLPersonsFilmsModel = ModelETL(
    index_name="persons",
    index_schema=indexes.PERSONS_INDEX,
    query=queries.last_modified_persons_films_query,
    amount_query_args=1,
    model=PersonFilms,
    batch_size=100,
)

storage_file_path = Path(Path(__file__).parents[1], "state", "state.json")


class AppSettings(BaseSettings):
    es_settings: ElasticConfig = ElasticConfig()
    etl_models: list[ModelETL] = [
        ETLPersonsFilmsModel,
        ETLGenreModel,
        ETLFilmModel,
    ]
    pg_settings: PostgresConfig = PostgresConfig()
    storage: State = State(JsonFileStorage(file_path=storage_file_path))
    sleep_interval: int = Field(120, env="TIME_INTERVAL")
    logger: logging.Logger = logger

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
