from functools import wraps
from time import sleep
from typing import Callable, Tuple

from components.logger import logger

EXECUTION_ERROR = "Произошла ошибка при выполнении функции {name}: {error}."
MAX_ATTEMPTS = (
    "Превышено максимальное количество попыток восстановить соединение."
)


def backoff(
        exceptions: Tuple,
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

    def func_wrapper(func: Callable) -> Callable:
        @wraps(func)
        def inner(*args, **kwargs) -> Callable:
            sleep_time = start_sleep_time
            attempts = 1
            while attempts <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as error:
                    logger.error(
                        EXECUTION_ERROR.format(name=func.__name__, error=error)
                    )
                    if sleep_time < border_sleep_time:
                        sleep_time = sleep_time * (factor ** attempts)
                    else:
                        sleep_time = border_sleep_time
                    sleep(sleep_time)
                    attempts += 1
            logger.error(MAX_ATTEMPTS)

        return inner

    return func_wrapper
