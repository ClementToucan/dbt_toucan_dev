{% macro norm_text (column_name) -%}

  TRIM(
    REGEXP_REPLACE(
      NORMALIZE(
        LOWER({{ column_name }})
      , NFD)
    , r"\pM", '')
  )
{% endmacro %}