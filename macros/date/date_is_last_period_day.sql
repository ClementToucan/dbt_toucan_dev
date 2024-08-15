{% macro date_is_last_period_day(column_name, period) -%}

    {% if period == 'week' -%}

    CASE WHEN DATE({{ column_name }}) = DATE_SUB(DATE_TRUNC(DATE_ADD(DATE({{ column_name }}), INTERVAL 1 {{ period }}), {{ period }}(monday)), INTERVAL 1 DAY) THEN 1 ELSE 0 END

    {% endif -%}

    {% if period != 'week' -%}

    CASE WHEN DATE({{ column_name }}) = DATE_SUB(DATE_TRUNC(DATE_ADD(DATE({{ column_name }}), INTERVAL 1 {{ period }}), {{ period }}), INTERVAL 1 DAY) THEN 1 ELSE 0 END

    {% endif -%}


{% endmacro %}