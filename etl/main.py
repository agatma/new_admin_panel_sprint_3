from time import sleep

from components.config import etl_settings, pg_settings
from components.logger import logger
from etl import ETL

ERROR_MESSAGE = "ETL процесс остановлен. Произошла ошибка: {error}."


def main():
    while True:
        etl = ETL()
        try:
            etl.load_data_from_postgres_to_elastic()
        except Exception as error:
            logger.exception(ERROR_MESSAGE.format(error=error))
        finally:
            logger.info("Остановка процесса на 5 минут")
            sleep(etl_settings.TIME_INTERVAL)


if __name__ == "__main__":
    main()
    print(pg_settings)
