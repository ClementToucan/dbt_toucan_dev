{% macro add_instance_freshness_flags(date, nb_of_days) -%}

  CASE
    WHEN DATE({{ date }}) BETWEEN CURRENT_DATE() - {{ nb_of_days }} AND CURRENT_DATE() THEN FALSE
    ELSE TRUE
  END

{% endmacro %}