{% macro salesloft_assign_group(column_name) -%}

  CASE WHEN {{ column_name }} = 6171 THEN "Team US"
  WHEN {{ column_name }} = 6172 THEN "Team EU"
  WHEN {{ column_name }} = 299 THEN "SDR FR"
  WHEN {{ column_name }} = 493 THEN "SDR EU"
  WHEN {{ column_name }} = 6170 THEN "AE EU"
  WHEN {{ column_name }} = 6169 THEN "AE US"
  WHEN {{ column_name }} = 298 THEN "SDR US"
  ELSE "Other"   END

{% endmacro %}