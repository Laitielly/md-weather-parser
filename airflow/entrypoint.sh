#!/bin/bash

# Создаем директорию и даем права
mkdir -p /opt/***/dbt/target
chown -R airflow:root /opt/***/dbt/target
chmod -R u+rwX /opt/***/dbt/target

# Оригинальный entrypoint
exec /entrypoint "$@"