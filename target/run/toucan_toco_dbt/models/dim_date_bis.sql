
  
    

    create or replace table `data-finance-staging`.`dbt_clement_msika`.`dim_date_bis`
      
    
    

    OPTIONS()
    as (
      

SELECT
  day,
  is_month_last_day,
  year,
  -- format quarter
  quarter_number,
 EXTRACT(year FROM day) || '-Q' || CAST(EXTRACT(quarter FROM day) AS STRING)

     AS quarter,
  
  -- format month
  month_number,
EXTRACT(year FROM day) || '-' ||
        CASE WHEN LENGTH(CAST(EXTRACT(month FROM day) AS STRING))>1 
            THEN CAST(EXTRACT(month FROM day) AS STRING)
        ELSE '0' || CAST(EXTRACT(month FROM day) AS STRING)
        END
     AS month,
  
  -- format week
  week_number,
  EXTRACT(year FROM day) || '-w' ||
        CASE WHEN LENGTH(CAST(EXTRACT(week(monday) FROM day) AS STRING))>1 
            THEN CAST(EXTRACT(week(monday) FROM day) AS STRING)
        ELSE '0' || CAST(EXTRACT(week(monday) FROM day) AS STRING)
        END

     AS week,

  -- Date parts

  CASE WHEN DATE_TRUNC(DATE(day), year) = DATE_TRUNC(DATE(CURRENT_DATE()), year) THEN 1 ELSE 0 END

      as date_is_current_year,
  CASE WHEN DATE_TRUNC(DATE(day),year) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 year), year) THEN 1 ELSE 0 END

     as date_is_last_year,
  CASE WHEN DATE_TRUNC(DATE(day),year) = DATE_TRUNC(DATE_ADD(DATE(CURRENT_DATE()), INTERVAL 1 year), year) THEN 1 ELSE 0 END

     as date_is_next_year,

  CASE WHEN DATE_TRUNC(DATE(day), quarter) = DATE_TRUNC(DATE(CURRENT_DATE()), quarter) THEN 1 ELSE 0 END

     as date_is_current_quarter,
  CASE WHEN DATE_TRUNC(DATE(day),quarter) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 quarter), quarter) THEN 1 ELSE 0 END

     as date_is_last_quarter,
  CASE WHEN DATE_TRUNC(DATE(day),quarter) = DATE_TRUNC(DATE_ADD(DATE(CURRENT_DATE()), INTERVAL 1 quarter), quarter) THEN 1 ELSE 0 END

     as date_is_next_quarter,
  
  CASE WHEN DATE_TRUNC(DATE(day), month) = DATE_TRUNC(DATE(CURRENT_DATE()), month) THEN 1 ELSE 0 END

     as date_is_current_month,
  CASE WHEN DATE_TRUNC(DATE(day),month) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 month), month) THEN 1 ELSE 0 END

     as date_is_last_month,
  CASE WHEN DATE_TRUNC(DATE(day),month) = DATE_TRUNC(DATE_ADD(DATE(CURRENT_DATE()), INTERVAL 1 month), month) THEN 1 ELSE 0 END

     as date_is_next_month,
  
  CASE WHEN DATE_TRUNC(DATE(day),week(monday)) = DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 week), week(monday)) THEN 1 ELSE 0 END

     as date_is_last_week,
  CASE WHEN DATE_TRUNC(DATE(day), week(monday)) = DATE_TRUNC(DATE(CURRENT_DATE()), week(monday)) THEN 1 ELSE 0 END

     as date_is_current_week,
  CASE WHEN DATE_TRUNC(DATE(day),week(monday)) = DATE_TRUNC(DATE_ADD(DATE(CURRENT_DATE()), INTERVAL 1 week), week(monday)) THEN 1 ELSE 0 END

     as date_is_next_week,


  CASE
            WHEN DATE(day) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 week), week(monday)) THEN 1
            ELSE 0 END
             as date_is_after_this_week,
  CASE
                    WHEN DATE(day) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 month), month) THEN 1
                    ELSE 0 END
             as date_is_after_this_month,
  CASE
                    WHEN DATE(day) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 quarter), quarter) THEN 1
                    ELSE 0 END
             as date_is_after_this_quarter,
  CASE
                    WHEN DATE(day) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 year), year) THEN 1
                    ELSE 0 END
             as date_is_after_this_year,

  CASE WHEN DATE(day) = DATE_SUB(DATE_ADD(DATE_TRUNC(day, year), INTERVAL 1 year), INTERVAL 1 DAY) THEN 1 ELSE 0 END

 as date_is_last_day_of_year,
  CASE WHEN DATE(day) = DATE_SUB(DATE_ADD(DATE_TRUNC(day, month), INTERVAL 1 month), INTERVAL 1 DAY) THEN 1 ELSE 0 END

 as date_is_last_day_of_month,
  CASE WHEN DATE(day) = DATE_SUB(DATE_ADD(DATE_TRUNC(day, quarter), INTERVAL 1 quarter), INTERVAL 1 DAY) THEN 1 ELSE 0 END

 as date_is_last_day_of_quarter,





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
    );
  