from typing import List, Optional

from psycopg2.extras import RealDictRow
from pydantic.error_wrappers import ValidationError

from components.logger import logger
from components.models import FilmWork
from components.storage import storage


def validate_film_works(
    rows: List[Optional[RealDictRow]],
) -> List[Optional[FilmWork]]:
    """Валидация фильмов перед загрузкой в Elasticsearch"""

    result, errors, success_id, = [], {}, set()
    for row in rows:
        try:
            result.append(FilmWork(**row))
            success_id.add(row["id"])
        except ValidationError as err:
            errors[row["id"]] = str(err)
    errors |= storage.get_state("pgsql_errors") or {}
    storage.set_state("pgsql_errors", errors)
    success = success_id.union(storage.get_state("pgsql_success") or set())
    storage.set_state("pgsql_success", list(success))
    logger.info(f"Успешно загружено {len(success)} объектов")
    if errors:
        logger.error(
            (
                f"Произошла ошибка при валидации фильмов из PostgreSQL. "
                f"{list(errors.keys())}. Подробности: etl_status/state.json"
            )
        )
    return result
