{% macro extract_domain (column_name) -%}

REGEXP_EXTRACT({{ column_name }}, r'^(?:http:\/\/|www\.|https:\/\/)([^\/]+)')

{% endmacro %}