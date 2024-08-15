{% macro format_date_quarter(column_name, period) -%}
    {% if period == 'year'  -%}

        EXTRACT(year FROM {{ column_name }})

    {% endif -%}

    
    {% if period == 'quarter'  -%}

        EXTRACT(year FROM {{ column_name }}) || '-Q' || CAST(EXTRACT(quarter FROM {{ column_name }}) AS STRING)

    {% endif -%}

    
    {% if period == 'month' -%}

        EXTRACT(year FROM {{ column_name }}) || '-' ||
        CASE WHEN LENGTH(CAST(EXTRACT(month FROM {{ column_name }}) AS STRING))>1 
            THEN CAST(EXTRACT(month FROM {{ column_name }}) AS STRING)
        ELSE '0' || CAST(EXTRACT(month FROM {{ column_name }}) AS STRING)
        END
    {% endif -%}

      {% if period == 'week' -%}

        EXTRACT(year FROM {{ column_name }}) || '-w' ||
        CASE WHEN LENGTH(CAST(EXTRACT(week(monday) FROM {{ column_name }}) AS STRING))>1 
            THEN CAST(EXTRACT(week(monday) FROM {{ column_name }}) AS STRING)
        ELSE '0' || CAST(EXTRACT(week(monday) FROM {{ column_name }}) AS STRING)
        END

    {% endif -%}


{% endmacro %}