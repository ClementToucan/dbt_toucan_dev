{% macro date_in_last_period(column_name, period, period_interval, interval_number) -%}


    {% if period == 'week' -%}

    CASE WHEN DATE_TRUNC(DATE({{ column_name }}),{{ period }}(monday)) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ interval_number }} {{ period_interval }}), {{ period }}(monday)) THEN 1 ELSE 0 END

    {% endif -%}

    {% if period != 'week' -%}

    CASE WHEN DATE_TRUNC(DATE({{ column_name }}),{{ period }}) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ interval_number }} {{ period_interval }}), {{ period }}) THEN 1 ELSE 0 END

    {% endif -%}

{% endmacro %}