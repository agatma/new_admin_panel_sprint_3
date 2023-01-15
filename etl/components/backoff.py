import time
from functools import wraps
from typing import Any

from _collections_abc import Callable

from clients.base_client import AbstractClientInterface
from components.logger import logger

EXECUTION_ERROR = "Произошла ошибка при выполнении функции {name}: {error}."
MAX_ATTEMPTS = (
    "Превышено максимальное количество попыток восстановить соединение."
)


def reconnect(func: Callable) -> Any:
    """Переподключение к клиенту в случае ошибки."""

    @wraps(func)
    def wrapper(storage: AbstractClientInterface, *args, **kwargs):
        if not storage.is_connected:
            logger.warning(
                f"Потеряли соединение к {storage}. "
                f"Попытка восстановить соединение..."
            )
            storage.reconnect()

        return func(storage, *args, **kwargs)

    return wrapper


def backoff(
    exceptions: tuple,
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
    max_attempts: int = 10,
) -> Callable:
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    :param exceptions: исключения, которые могут произойти
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param max_attempts: максимальное количество попыток
    """

    def func_wrapper(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as error:
                logger.error(
                    EXECUTION_ERROR.format(
                        name=func.__name__, error=type(error)
                    )
                )
                t = start_sleep_time
                n = 0
                attempts = 1
                while attempts <= max_attempts:
                    try:
                        return func(*args, **kwargs)
                    except exceptions as error:
                        if t < border_sleep_time:
                            t = t * factor**n
                            n += 1
                        delay = min(t, border_sleep_time)
                        logger.error(
                            EXECUTION_ERROR.format(
                                name=func.__name__, error=type(error)
                            )
                        )
                        time.sleep(delay)
                        attempts += 1
                if attempts >= max_attempts:
                    raise ConnectionError("Не удалось установить соединение ")

        return inner

    return func_wrapper
