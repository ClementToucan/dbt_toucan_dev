{% macro analytics_convert_date(column_name) -%}

  CASE WHEN REGEXP_CONTAINS({{ column_name }} , r'^\d{4}-\d{2}-\d{2}')
  THEN TIMESTAMP({{ column_name }})
  ELSE TIMESTAMP_MILLIS(CAST({{ column_name }} AS INT64)) END

{% endmacro %}