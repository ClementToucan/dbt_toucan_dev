{% macro add_dbt_last_run_ts() -%}

TIMESTAMP(DATETIME(CURRENT_TIMESTAMP())) AS last_dbt_run,
TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), 'US/Eastern')) AS last_dbt_run_us_eastern_time,
TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), 'Europe/Paris')) AS last_dbt_run_eu_paris_time,

{% endmacro %}