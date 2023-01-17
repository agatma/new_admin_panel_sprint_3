from typing import Optional
from collections import defaultdict
from uuid import UUID

from pydantic import BaseModel, BaseSettings, Field, validator


class BaseModelUUIDMixin(BaseModel):
    id: UUID


class GenreShort(BaseModelUUIDMixin):
    name: str


class Genre(GenreShort):
    description: str | None = None


class Person(BaseModelUUIDMixin):
    full_name: str


class PersonFilms(Person):
    films: list = []

    @validator("films")
    def validate_films(cls, v):
        roles = defaultdict(list)
        for row in v:
            role = row.pop("role")
            roles[role].append(Film(**row))
        return roles


class Film(BaseModelUUIDMixin):
    id: UUID = Field(alias="uuid")
    title: str
    imdb_rating: float


class FilmWork(BaseModelUUIDMixin):
    imdb_rating: float
    age_limit: Optional[int] = 18
    genres: list[GenreShort]
    title: str
    description: str | None
    directors_names: list[str | None]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]


class ModelETL(BaseSettings):
    index_name: str
    index_schema: dict
    query: str
    amount_query_args: int
    model: type[BaseModel]
    batch_size: int
