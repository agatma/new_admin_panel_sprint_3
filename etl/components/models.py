from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class Person(BaseModel):
    id: UUID
    name: str


class FilmWork(BaseModel):
    id: UUID
    imdb_rating: float
    genre: List[str]
    title: str
    description: Optional[str]
    director: List[Optional[str]]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]
