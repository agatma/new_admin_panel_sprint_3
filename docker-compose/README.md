# Реализация API для кинотеатра

Цель проекта: создать API, возвращающий список фильмов в формате, описанном в openapi-файле, и позволяющий получить информацию об одном фильме. Дополнительно реализован полнотекстовый поиск


### Работа с проектом используя make
#### Список контейнеров: postgres, django-app, elasticsearch, ETL-app, swagger, nginx

```
make full_run		- Запуск контейнеров с миграциями и бекапом
make run		- Пустой запуск контейнеров
make migrate		- Выполнить миграции
make load		- Загрузить данные в postgresql
make superuser		- Создать суперпользователя
make backend		- Консоль backend контейнера.
make etl		- Консоль ETL контейнера.
make postgresql		- Консоль postgresql контейнера.
make nginx		- Консоль nginx контейнера.
make stop		- Выключение контейнера.
```
