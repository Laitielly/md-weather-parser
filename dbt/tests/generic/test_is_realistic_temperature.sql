{% test is_realistic_temperature(model, column_name) %}

with validation as (
    select
        {{ column_name }} as temp_field
    from {{ model }}
),

validation_errors as (
    select
        temp_field
    from validation
    where temp_field < -60 or temp_field > 60
)

select *
from validation_errors

{% endtest %}