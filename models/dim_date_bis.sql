{{ config(materialized='table') }}

SELECT
  day,
  is_month_last_day,
  year,
  -- format quarter
  quarter_number,
 {{ format_date_quarter('day', 'quarter') }} AS quarter,
  
  -- format month
  month_number,
{{ format_date_quarter('day', 'month') }} AS month,
  
  -- format week
  week_number,
  {{ format_date_quarter('day', 'week') }} AS week,

  -- Date parts

  {{ date_in_current_period('day', 'year') }}  as date_is_current_year,
  {{ date_in_last_period('day', 'year','year', 1) }} as date_is_last_year,
  {{ date_in_next_period('day', 'year','year', 1) }} as date_is_next_year,

  {{ date_in_current_period('day', 'quarter') }} as date_is_current_quarter,
  {{ date_in_last_period('day', 'quarter','quarter', 1) }} as date_is_last_quarter,
  {{ date_in_next_period('day', 'quarter','quarter', 1)}} as date_is_next_quarter,
  
  {{ date_in_current_period('day', 'month') }} as date_is_current_month,
  {{ date_in_last_period('day', 'month','month', 1) }} as date_is_last_month,
  {{ date_in_next_period('day', 'month','month', 1)}} as date_is_next_month,
  
  {{ date_in_last_period('day', 'week','week', 1) }} as date_is_last_week,
  {{ date_in_current_period('day', 'week')}} as date_is_current_week,
  {{ date_in_next_period('day', 'week','week', 1)}} as date_is_next_week,


  {{ date_after_current_period('day', 'week') }} as date_is_after_this_week,
  {{ date_after_current_period('day', 'month') }} as date_is_after_this_month,
  {{ date_after_current_period('day', 'quarter') }} as date_is_after_this_quarter,
  {{ date_after_current_period('day', 'year') }} as date_is_after_this_year,

  {{ date_is_last_day_of_period('day', 'year') }} as date_is_last_day_of_year,
  {{ date_is_last_day_of_period('day', 'month') }} as date_is_last_day_of_month,
  {{ date_is_last_day_of_period('day', 'quarter') }} as date_is_last_day_of_quarter,





FROM (
  SELECT
    day,
    -- year
    EXTRACT(year
    FROM
      day) AS year,
    -- get quarter
    EXTRACT(QUARTER
    FROM
      day) AS quarter_number,
    -- get month
    EXTRACT(month
    FROM
      day) AS month_number,
    -- is last day of the month
    CASE
      WHEN day = DATE_SUB(DATE_TRUNC(DATE_ADD(day, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) THEN TRUE
    ELSE
    FALSE
  END
    AS is_month_last_day,
    -- get week (EU)
    EXTRACT(week
    FROM
      day) AS week_number
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01',DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH),
 INTERVAL 1 DAY), INTERVAL 1 day)) day
  ORDER BY
    day DESC )
