from uuid import UUID
from collections import defaultdict
from pydantic import BaseSettings, Field
from pydantic import BaseModel, validator


class Genre(BaseModel):
    id: UUID
    name: str
    description: str | None = None


class Person(BaseModel):
    id: UUID
    name: str


class FilmShort(BaseModel):
    id: UUID = Field(alias="uuid")
    title: str
    imdb_rating: float


class PersonFilms(BaseModel):
    id: UUID
    full_name: str
    films: list = []

    @validator('films')
    def validate_films(cls, v):
        roles = defaultdict(list)
        for row in v:
            role = row.pop('role')
            roles[role].append(FilmShort(**row))
        return roles


class FilmWork(BaseModel):
    id: UUID
    imdb_rating: float
    genre: list[str]
    title: str
    description: str | None
    director: list[str | None]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]


class ModelETL(BaseSettings):
    index_name: str
    index_schema: dict
    query: str
    amount_query_args: int
    model: type[BaseModel]
    batch_size: int
