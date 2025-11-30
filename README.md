## Структура проекта:
root/
├─ .env
├─ .gitignore
├─ docker-compose.yml
├─ README.md
├─ app/
│ ├─ Dockerfile
│ ├─ requirements.txt
│ └─ main.py
├─ airflow/
│ ├─ Dockerfile
│ ├─ requirements-airflow.txt
│ ├─ dags/
│ │ └─ mongo_to_postgres_dag.py
│ ├─ logs/
│ └─ plugins/
└─ scripts/
└─ init_db_analytics.sql
