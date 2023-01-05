import logging
import sys
from datetime import datetime
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

logger = logging.getLogger("etl")
logger.setLevel(logging.DEBUG)
time_rotated_handler = TimedRotatingFileHandler(
    backupCount=7,
    encoding="utf-8",
    filename=Path(
        Path(__file__).parents[1], "logs", "etl.log".format(datetime.now())
    ),
    interval=1,
    when="midnight",
)

LOGS_FORMAT = (
    "| %(asctime)s – [%(levelname)s]: %(message)s. "
    "Исполняемый файл – '%(filename)s': "
    "функция – '%(funcName)s'(%(lineno)d)"
)

time_rotated_handler.setFormatter(Formatter(LOGS_FORMAT))
logger.addHandler(time_rotated_handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
