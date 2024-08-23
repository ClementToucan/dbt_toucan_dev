{{ config(materialized='table') }}


WITH user_table AS (


SELECT

    distinct_id AS user_id,

    time

FROM data-finance-staging.mixpanel.raw__mp_master_event


), 


cohort_table AS (

SELECT

    user_id,

    MIN(DATE(time)) AS cohort_date 


FROM user_table

GROUP BY 1


),

activity_table AS (

SELECT

    user_id,

    DATE(time) AS activity_date 

FROM user_table

GROUP BY 1,2



),

first_retention_table AS (

SELECT

    a.user_id,

    a.cohort_date,

    activity_date,

    CASE 
    WHEN DATE_DIFF(activity_date, cohort_date, DAY) = 365 THEN 1 ELSE 0
    END AS is_retained_year_one,

    CASE 
    WHEN DATE_DIFF(activity_date, cohort_date, DAY) = 30 THEN 1 ELSE 0
    END AS is_retained_day_thirty,

    CASE 
    WHEN DATE_DIFF(activity_date, cohort_date, DAY) = 14 THEN 1 ELSE 0
    END AS is_retained_day_fourteen,

    CASE 
    WHEN DATE_DIFF(activity_date, cohort_date, DAY) = 7 THEN 1 ELSE 0
    END AS is_retained_day_seven,

    CASE 
    WHEN DATE_DIFF(activity_date, cohort_date, DAY) = 1 THEN 1 
    ELSE 0
    END AS is_retained_day_one


FROM cohort_table a

LEFT JOIN activity_table b ON b.user_id = a.user_id

),

week_table AS (


SELECT

DISTINCT

    year AS y,

    week_number

FROM `utils.dim_date`

WHERE DATE(day) BETWEEN (SELECT MIN(cohort_date) FROM first_retention_table) AND (SELECT MAX(cohort_date) FROM first_retention_table)


),

second_retention_table AS (

SELECT

    FORMAT_DATE('%Y', DATE(cohort_date)) AS y,

    FORMAT_DATE('%V', DATE(cohort_date)) AS cohort_week_number,

    DATE_TRUNC(DATE(cohort_date), WEEK) AS week_start,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 1 THEN user_id ELSE NULL END)) AS new_users_day_one,

    COUNT(DISTINCT(CASE WHEN is_retained_day_one = 1 AND DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 1 THEN user_id ELSE NULL END)) AS nb_users_retained_day_one,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 7 THEN user_id ELSE NULL END)) AS new_users_day_seven,

    COUNT(DISTINCT(CASE WHEN is_retained_day_seven = 1 AND DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 7 THEN user_id ELSE NULL END)) AS nb_users_retained_day_seven,

    COUNT(DISTINCT(CASE WHEN is_retained_day_fourteen = 1 AND DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 14 THEN user_id ELSE NULL END)) AS nb_users_retained_day_fourteen,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 14 THEN user_id ELSE NULL END)) AS new_users_day_fourteen,

    COUNT(DISTINCT(CASE WHEN is_retained_day_thirty = 1 AND DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 30 THEN user_id ELSE NULL END)) AS nb_users_retained_day_thirty,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 30 THEN user_id ELSE NULL END)) AS new_users_day_thirty,

    COUNT(DISTINCT(CASE WHEN is_retained_year_one = 1 AND DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 365 THEN user_id ELSE NULL END)) AS nb_users_retained_year_one,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(CURRENT_DATE(), activity_date, DAY) >= 365 THEN user_id ELSE NULL END)) AS new_users_year_one



FROM first_retention_table

GROUP BY 1,2,3

ORDER BY 1

)

SELECT

    a.y,

    a.week_number AS cohort_week_number,

    CASE WHEN new_users_day_one > 0 THEN CAST(nb_users_retained_day_one AS FLOAT64) / CAST(new_users_day_one AS FLOAT64)
    ELSE NULL
    END AS retention_day_one,

    new_users_day_one,

    nb_users_retained_day_one,

    CASE WHEN new_users_day_seven > 0 THEN CAST(nb_users_retained_day_seven AS FLOAT64) / CAST(new_users_day_seven AS FLOAT64)
    ELSE NULL
    END AS retention_day_seven,

    new_users_day_seven,

    nb_users_retained_day_seven,

    CASE WHEN new_users_day_fourteen > 0 THEN CAST(nb_users_retained_day_fourteen AS FLOAT64) / CAST(new_users_day_fourteen AS FLOAT64)
    ELSE NULL
    END AS retention_day_fourteen,

    new_users_day_fourteen,

    nb_users_retained_day_fourteen,

    CASE WHEN new_users_day_thirty > 0 THEN CAST(nb_users_retained_day_thirty AS FLOAT64) / CAST(new_users_day_thirty AS FLOAT64)
    ELSE NULL
    END AS retention_day_thirty,

    new_users_day_thirty,

    nb_users_retained_day_thirty,

    CASE WHEN new_users_year_one > 0 THEN CAST(nb_users_retained_year_one AS FLOAT64) / CAST(new_users_year_one AS FLOAT64)
    ELSE NULL
    END AS retention_year_one,

    new_users_year_one,

    nb_users_retained_year_one




FROM week_table a 

LEFT JOIN  second_retention_table b ON CAST(b.y AS INT) = CAST(a.y AS INT) AND CAST(a.week_number AS INT) = CAST(b.cohort_week_number AS INT)


ORDER BY 1,2
