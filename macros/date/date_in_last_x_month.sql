{% macro date_in_last_x_month(column_name, number) -%}

case when date ({{ column_name }}) BETWEEN DATE_SUB(current_date(), INTERVAL {{ number }} MONTH) and current_date() then 1 else 0 end 

{% endmacro %}