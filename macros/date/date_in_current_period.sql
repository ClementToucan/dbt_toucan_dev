{% macro date_in_current_period(column_name, period) -%}

    {% if period == 'week' -%}


    CASE WHEN DATE_TRUNC(DATE({{ column_name }}), {{ period }}(monday)) = DATE_TRUNC(DATE(CURRENT_DATE()), {{ period }}(monday)) THEN 1 ELSE 0 END

    {% endif -%}

    {% if period != 'week' -%}


    CASE WHEN DATE_TRUNC(DATE({{ column_name }}), {{ period }}) = DATE_TRUNC(DATE(CURRENT_DATE()), {{ period }}) THEN 1 ELSE 0 END

    {% endif -%}

{% endmacro %}