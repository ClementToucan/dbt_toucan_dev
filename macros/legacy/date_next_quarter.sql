{% macro date_next_quarter(column_name) -%}
case when extract (quarter from closedate) in (1,2,3) 
then 
EXTRACT(year FROM {{ column_name }}) || '-Q' || CAST(EXTRACT(quarter FROM {{ column_name }}) +1 AS STRING)
else 
concat(CAST(EXTRACT(year from closedate) +1 as STRING),'-Q', 1)  
end
{% endmacro %}