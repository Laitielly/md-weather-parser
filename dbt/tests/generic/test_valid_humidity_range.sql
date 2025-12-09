{% test valid_humidity_range(model, column_name) %}
with validation as (
    select {{ column_name }} as humidity_value
    from {{ model }}
),
validation_errors as (
    select humidity_value
    from validation
    where humidity_value < 0 or humidity_value > 100
)
select *
from validation_errors
{% endtest %}