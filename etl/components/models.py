from uuid import UUID

from pydantic import BaseModel


class Person(BaseModel):
    id: UUID
    name: str


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
