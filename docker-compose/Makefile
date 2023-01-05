PROJECT_NAME = Movies

all:
	@echo "make full_run		- Запуск контейнеров с миграциями и бекапом"
	@echo "make full_run		- Пустой запуск контейнеров"
	@echo "make migrate		- Выполнить миграции "
	@echo "make load		- Загрузить данные в postgresql "
	@echo "make superuser		- Создать суперпользователя "
	@echo "make backend		- Консоль backend контейнера."
	@echo "make etl		- Консоль ETL контейнера."
	@echo "make postgresql		- Консоль postgresql контейнера."
	@echo "make nginx		- Консоль nginx контейнера."
	@echo "make stop		- Выключение контейнера."

full_run:
	#docker-compose up Команда для сборки и запуска контейнера.
	#--build Сборка образа перед запуском контейнера.
	#-d Detached mode.
	docker-compose up --build -d

	#Подключение к консоли веб-контейнера для создания миграций.
	docker-compose run backend python manage.py migrate

	#Подключение к консоли веб-контейнера для загрузки бэкапа в базу данных.
	docker-compose run backend python manage.py loaddata dump.json

run:
    #docker-compose up Команда для сборки и запуска контейнера.
	#--build Сборка образа перед запуском контейнера.
	#-d Detached mode.
	docker-compose up --build -d


backend:
	#Запуск консоли для управления веб-контейнером.
	docker-compose exec backend bash

etl:
    #Запуск консоли для управления etl-контейнером.
	docker-compose exec etl bash

postgresql:
	#Запуск консоли для управления PostgreSQL-контейнером.
	docker-compose exec db bash

nginx:
	#Запуск консоли для управления nginx-контейнером.
	docker-compose exec nginx bash

stop:
	#Остановка и удаление контейнеров, запущенных docker-compose up.
	docker-compose down

migrate:
	#Подключение к консоли веб-контейнера для создания миграций.
	docker-compose run backend python manage.py migrate

load:
	#Подключение к консоли веб-контейнера для загрузки бэкапа в базу данных.
	docker-compose run backend python manage.py loaddata dump.json

superuser:
	#Подключение к консоли веб-контейнера для создания Суперпользователя(администратор).
	docker-compose run backend python manage.py createsuperuser

