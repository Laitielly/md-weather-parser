# Аналитический конвейер на базе Apache Airflow и Docker

Данный репозиторий содержит платформу для сбора, оркестрации и аналитической загрузки данных о температуре в Калининграде, реализованную с использованием контейнеризированного микросервисного подхода. Система автоматически собирает текущую погоду и прогнозы, оценивает точность прогнозов и предоставляет аналитические данные для мониторинга качества метеорологических предсказаний.

## Структура проекта
```
md-weather-parser/
├── .env # Переменные окружения
├── .gitignore # Игнорируемые файлы Git
├── .pre-commit-config.yaml # Настройки pre-commit
├── .pylintrc # Конфигурация линтера
├── docker-compose.yml # Docker Compose для разработки
├── docker-compose.prod.yml # Docker Compose для продакшена
├── README.md # Документация
├── seed.js # Скрипт генерации тестовых данных
│
├── app/ # Сервис сбора данных
│ ├── Dockerfile
│ ├── main.py
│ └── requirements.txt
│
├── airflow/ # Airflow оркестрация
│ ├── Dockerfile
│ ├── entrypoint.sh
│ ├── requirements.txt
│ └── dags/
│ ├── collect_weather_dag.py
│ ├── weather_el.py
│ └── dbt_pipeline.py
│
└── dbt/ # DBT трансформации
```

## Архитектура и компоненты

Проект развернут через **Docker Compose** и представляет собой стек из шести взаимосвязанных сервисов:

| Сервис | Технология | Роль |
| :--- | :--- | :--- |
| `app` | FastAPI (Python 3.11) | Сбор данных. Получает текущую погоду и 48-часовой прогноз для Калининграда через OpenWeatherMap API. |
| `mongodb` | Mongo 6 | Landing Zone. Хранение неструктурированных данных. |
| `postgres_analytics` | PostgreSQL 14 | Data Warehouse. Аналитическое хранилище структурированных данных (таблица `analytics.tickets`). |
| `Airflow Stack` | Apache Airflow | Оркестрация (ELT). Управляет DAGs для перемещения данных и администрирования. |
| `postgres` | PostgreSQL 14 | База метаданных Airflow. |
| `redis` | Redis 7 | Брокер сообщений для Celery Executor Airflow. |

## Data pipeline

### 1. **Сбор данных** (`collect_weather_data` DAG)
- **Расписание**: Каждый час (`0 * * * *`)
- **Действия**: Сбор текущей погоды и 48-часового прогноза через OpenWeatherMap API
- **Хранение**: Данные сохраняются в MongoDB (коллекции `current_weather` и `forecasts`)
- **Отказоустойчивость**: Использует мокированные данные при недоступности API

### 2. **EL процесс** (`weather_el` DAG)
- **Триггер**: Запускается после успешного сбора данных
- **Extract**: Извлечение данных из MongoDB за последние 3 часа
- **Load**: Загрузка в raw-слой PostgreSQL (`raw_data.current_weather`, `raw_data.forecasts`)

### 3. **DBT трансформации** (`dbt_weather_pipeline` DAG)
- **Триггер**: Запускается после EL процесса
- **Staging**: Очистка и типизация данных (`stg` слой)
- **ODS**: Формирование оперативного хранилища (`ods` слой)
- **Data Marts**: Создание аналитических витрин (`dm` слой)
- **Тестирование**: Проверка качества данных с помощью Elementary
- **Документация**: Генерация отчетов и документации


## Инструкция по запуску

Для развертывания платформы требуется установленный Docker и Docker Compose.

### Запуск и инициализация

Выполните команду:

```bash
docker-compose up -d --build
```

> Примечание: Для гарантии чистой инициализации базы данных (PostgreSQL и MongoDB), особенно после изменений в `.env` или предыдущих ошибок, используйте флаг `-v` (удаление томов): `docker-compose down -v && docker-compose up -d --build`.

### Доступ к интерфейсам

| Название                      | Порт  | Адрес                                    | Учетные данные          |
| :---------------------------- | :---- | :--------------------------------------- | :---------------------- |
| Airflow Webserver             | 8080  | [ссылка](http://89.169.170.56:8080/home) | admin / admin           |
| FastAPI App                   | 8081  | [ссылка](http://89.169.170.56:8081/docs) | N/A                     |
| Elementary Dashboard | 8081 | [ссылка](http://89.169.170.56:8081/elementary/report)     | N/A |
| Postgres Analytics | 5433  | [ссылка](http://89.169.170.56:5433/docs) | readonly / readonlypass |
| Mongo | 27017 | [ссылка](http://89.169.170.56:27017)     | readonly / readonlypass |

---

### URL подключения

**MongoDB:**

Доступные юзеру коллекции: 
 - current_weather
 - forecasts

Пример:
```
In: use weather_db
Out: switched to db weather_db

In: <weather_db> db.current_weather.findOne()
Out:
{
  _id: 'f8d1f253-17dc-412c-8ad2-bdfd152d34e6',
  city: 'Kaliningrad',
  country: 'RU',
  dt: 1764872170,
  temp: 18.7,
  feels_like: 16,
  pressure: 1009,
  humidity: 66,
  wind_speed: 1.9,
  wind_deg: 54,
  weather_main: 'Clouds',
  weather_description: 'снег',
  clouds: 95,
  visibility: 7857,
  collected_ts: ISODate('2025-12-04T18:16:10.443Z')
}
```

```env
MONGO_INITDB_ROOT_USERNAME=readonly
MONGO_INITDB_ROOT_PASSWORD=readonlypass
MONGO_HOST=89.169.170.56
MONGO_PORT=27017
MONGO_INITDB_DATABASE=weather_db

MONGO_URL=mongodb://readonly:readonlypass@89.169.170.56:27017/weather_db?authSource=weather_db
```

**Postgres Analytics:**

```env
POSTGRES_USER=readonly
POSTGRES_PASSWORD=readonlypass
POSTGRES_HOST=89.169.170.56
POSTGRES_PORT=5433
POSTGRES_DB=analytics

POSTGRES_URL=postgresql://readonly:readonlypass@89.169.170.56:5433/analytics
```
