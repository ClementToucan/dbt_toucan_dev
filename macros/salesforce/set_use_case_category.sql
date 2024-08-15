{% macro set_use_case_category(use_case) -%}

CASE WHEN {{ use_case }} = 'Embedded Software (OEM)' THEN 'Embedded Software (OEM)'
WHEN {{ use_case }} = 'Customer Web Experience' THEN 'Customer Web Experience'
ELSE 'Opportunistic' END

{% endmacro %}