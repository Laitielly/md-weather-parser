from __future__ import annotations
import os
import json
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")

PG_HOST = os.environ.get("POSTGRES_ANALYTICS_HOST", "postgres_analytics")
PG_DB = os.environ.get("POSTGRES_ANALYTICS_DB", "analytics")
PG_USER = os.environ.get("POSTGRES_ANALYTICS_USER", "pguser")
PG_PASSWORD = os.environ.get("POSTGRES_ANALYTICS_PASSWORD", "pgpass")
PG_PORT = int(os.environ.get("POSTGRES_ANALYTICS_PORT", 5432))


def create_raw_schema(**context):
    """Create the raw_data schema if it doesn't exist"""
    conn = _make_pg_conn()
    cur = conn.cursor()

    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
        log.info("Schema raw_data created or already exists")
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        log.info("Schema raw_data already exists")
    finally:
        cur.close()
        conn.close()


def _make_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _make_pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT
    )
    conn.autocommit = True
    return conn


def extract_current_weather(
    window_hours: int | None = 3, **context
) -> List[Dict[str, Any]]:
    client = _make_mongo_client()
    col = client["weather_db"]["current_weather"]

    find_query = {}

    if window_hours is not None:
        cutoff = datetime.utcnow() - timedelta(hours=window_hours)
        find_query = {"collected_ts": {"$gte": cutoff}}

    cursor = col.find(find_query)
    rows = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        rows.append(d)

    log.info("Extracted %s current weather rows from Mongo", len(rows))
    return rows


def extract_forecasts(window_hours: int | None = 3, **context) -> List[Dict[str, Any]]:
    client = _make_mongo_client()
    col = client["weather_db"]["forecasts"]

    find_query = {}

    if window_hours is not None:
        cutoff = datetime.utcnow() - timedelta(hours=window_hours)
        find_query = {"collection_dt": {"$gte": cutoff}}

    cursor = col.find(find_query)
    rows = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        rows.append(d)

    log.info("Extracted %s forecast rows from Mongo", len(rows))
    return rows


def load_raw_data(task_extract_id: str, table_name: str, **context):
    """Универсальная функция загрузки в raw слой"""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids=task_extract_id)

    if not rows:
        log.info(f"No rows to load into {table_name}")
        return

    conn = _make_pg_conn()
    cur = conn.cursor()

    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
        sql: Optional[str] = None
        vals: Optional[List] = None

        if table_name == "current_weather":
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data.current_weather (
                    id TEXT PRIMARY KEY,
                    doc JSONB NOT NULL,
                    loaded_at TIMESTAMPTZ DEFAULT NOW()
                );
            """
            )
            vals = [(str(r["_id"]), json.dumps(r, default=str)) for r in rows]
            sql = "INSERT INTO raw_data.current_weather (id, doc) VALUES %s ON CONFLICT (id) DO NOTHING"

        elif table_name == "forecasts":
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data.forecasts (
                    id TEXT PRIMARY KEY,
                    doc JSONB NOT NULL,
                    loaded_at TIMESTAMPTZ DEFAULT NOW()
                );
            """
            )
            vals = [(str(r["_id"]), json.dumps(r, default=str)) for r in rows]
            sql = "INSERT INTO raw_data.forecasts (id, doc) VALUES %s ON CONFLICT (id) DO NOTHING"

        psycopg2.extras.execute_values(cur, sql, vals, page_size=100)
        log.info(f"Loaded {cur.rowcount} rows into raw_data.{table_name}")

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="weather_el",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["weather", "el", "raw"],
) as dag:

    t_create_schema = PythonOperator(
        task_id="create_raw_schema",
        python_callable=create_raw_schema,
    )

    t_extract_current = PythonOperator(
        task_id="extract_current_weather",
        python_callable=extract_current_weather,
        op_kwargs={"window_hours": None},
    )

    t_extract_forecasts = PythonOperator(
        task_id="extract_forecasts",
        python_callable=extract_forecasts,
        op_kwargs={"window_hours": None},
    )

    t_load_current = PythonOperator(
        task_id="load_current_weather",
        python_callable=load_raw_data,
        op_kwargs={
            "task_extract_id": "extract_current_weather",
            "table_name": "current_weather",
        },
    )

    t_load_forecasts = PythonOperator(
        task_id="load_forecasts",
        python_callable=load_raw_data,
        op_kwargs={"task_extract_id": "extract_forecasts", "table_name": "forecasts"},
    )

    t_create_schema >> [t_extract_current, t_extract_forecasts]
    t_extract_current >> t_load_current
    t_extract_forecasts >> t_load_forecasts
