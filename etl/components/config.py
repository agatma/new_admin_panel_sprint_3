from pydantic import BaseSettings, Field


class PostgresConfig(BaseSettings):
    dbname: str = Field(..., env="POSTGRES_DB")
    user: str = Field(..., env="POSTGRES_USER")
    password: str = Field(..., env="POSTGRES_PASSWORD")
    host: str = Field(..., env="POSTGRES_HOST")
    port: int = Field(..., env="POSTGRES_PORT")
    options: str = Field(..., env="POSTGRES_OPTIONS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class ElasticConfig(BaseSettings):
    host: str = Field("localhost", env="ELASTIC_HOST")
    port: int = Field(9200, env="ELASTIC_PORT")
    index_name: str = Field("movies", env="INDEX_NAME")

    def elastic_url(self):
        return f"http://{self.host}:{self.port}/"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class ETLConfig(BaseSettings):
    BATCH_SIZE: int = Field(100, env="BATCH_SIZE")
    TIME_INTERVAL: int = Field(300, env="TIME_INTERVAL")
    STATE_FILE_NAME: str = Field('state.json', env="STATE_FILE_NAME")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


etl_settings = ETLConfig()
pg_settings = PostgresConfig()
es_settings = ElasticConfig()

