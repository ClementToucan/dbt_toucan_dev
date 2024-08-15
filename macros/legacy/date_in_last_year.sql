{% macro date_last_year (column_name) -%}

case when date ({{ column_name }}) BETWEEN DATE_SUB(current_date(), INTERVAL 12 month) and current_date() then 1 else 0 end 

{% endmacro %}