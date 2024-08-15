{% macro date_after_current_period(
        column_name,
        period) -%}
    {% if period == 'week' -%}
        CASE
            WHEN DATE({{ column_name }}) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 {{ period }}), {{ period }}(monday)) THEN 1
            ELSE 0 END
            {% endif -%}

            {% if period != 'week' -%}
                CASE
                    WHEN DATE({{ column_name }}) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 {{ period }}), {{ period }}) THEN 1
                    ELSE 0 END
            {% endif -%}
{% endmacro %}