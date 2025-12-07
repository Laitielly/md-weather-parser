-- Общие тесты для ключевых колонок
{% test not_null_columns(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}