from abc import ABC, abstractmethod
from typing import Any, Type

from pydantic import AnyUrl

from components.logger import logger


class AbstractClientInterface(ABC):
    base_exceptions: Type[BaseException] | tuple[Type[BaseException]] | Any

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def reconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class AbstractClient(AbstractClientInterface, ABC):
    _connection: Any

    def __init__(self, dsn: AnyUrl, *args, **kwargs):
        self.dsn = dsn
        self.args = args
        self.kwargs = kwargs
        self.connect()

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def reconnect(self) -> None:
        if not self.is_connected:
            logger.info(
                f"Пытаюсь переподключиться к {self.__class__.__name__}"
            )
            self.connect()

    @abstractmethod
    def close(self) -> None:
        if self.is_connected:
            self._connection.close()
            logger.info(f"Соединение закрыто для {self.__class__.__name__}")
        self._connection = None
