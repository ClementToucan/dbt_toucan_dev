{% macro date_in_ytd (column_name) -%}

case when date ({{ column_name }}) BETWEEN DATE_TRUNC(current_date(), year) and current_date() then 1 else 0 end 

{% endmacro %}