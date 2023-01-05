import abc
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, Optional

from components.config import etl_settings
from components.logger import logger

FILE_NOT_FOUND = "Файл {name} не найден. Произошла ошибка: {error}."
READ_ERROR = "Файл {name} не может быть прочитан. Произошла ошибка: {error}."


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: Dict) -> None:
        """Сохранить состояние в хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict:
        """Достать состояние из хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self._file_path = file_path

    def save_state(self, state: Dict) -> None:
        try:
            with open(self._file_path, "w") as write_file:
                json.dump(state, write_file, ensure_ascii=False)
        except FileNotFoundError as error:
            logger.debug(
                FILE_NOT_FOUND.format(name=self._file_path, error=error)
            )

    def retrieve_state(self) -> Dict:
        state = {}
        try:
            with open(self._file_path, "r") as read_file:
                state = json.load(read_file)
        except (JSONDecodeError, FileNotFoundError) as error:
            logger.debug(READ_ERROR.format(name=self._file_path, error=error))
        return state


class State:
    def __init__(self, storage: BaseStorage):
        self._storage = storage
        self._state = {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для ключа"""
        if self._state is not None:
            self._state[key] = value
            self._storage.save_state(self._state)

    def get_state(self, key: str) -> Any:
        """Достать состояние ключа"""
        self._state = self._storage.retrieve_state()
        return self._state.get(key)


file_path = Path(
    Path(__file__).parents[1], "state", etl_settings.STATE_FILE_NAME
)

storage = State(JsonFileStorage(file_path=file_path))
