from time import sleep

from services.ETL_pipeline import ETL
from components.config import etl_settings
from components.logger import logger

ERROR_MESSAGE = "ETL процесс остановлен. Произошла ошибка: {error}."


def main():
    while True:
        etl = ETL()
        try:
            etl.postgres.connect_to_postgres()
            etl.elastic.connect()
            etl.elastic.create_index()
            etl.load_data_from_postgres_to_elastic()
        except Exception as error:
            logger.error(ERROR_MESSAGE.format(error=error))
        finally:
            etl.postgres.close_connection()
            logger.info("Остановка процесса на 5 минут")
            sleep(etl_settings.TIME_INTERVAL)


if __name__ == "__main__":
    main()
