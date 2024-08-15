{% macro date_is_last_day_of_period(column_name, period) -%}

CASE WHEN DATE({{ column_name }}) = DATE_SUB(DATE_ADD(DATE_TRUNC({{ column_name }}, {{ period }}), INTERVAL 1 {{ period }}), INTERVAL 1 DAY) THEN 1 ELSE 0 END

{% endmacro %}