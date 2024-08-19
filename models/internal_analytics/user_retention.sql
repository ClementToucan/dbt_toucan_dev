{{ config(materialized='table')  }}

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

second_retention_table AS (

SELECT

    cohort_date,

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

GROUP BY 1

ORDER BY 1

)

SELECT

    cohort_date,

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




FROM second_retention_table


UNION ALL

SELECT

    day,

    NULL AS retention_day_one,

    NULL AS new_users_day_one,

    NULL AS nb_users_retained_day_one,

    NULL AS new_users_day_seven,

    NULL AS nb_users_retained_day_seven,

    NULL AS new_users_day_fourteen,

    NULL AS nb_users_retained_day_fourteen,

    NULL AS new_users_day_thirty,

    NULL AS nb_users_retained_day_thirty,

    NULL AS new_users_year_one,

    NULL AS nb_users_retained_year_one

FROM `utils.dim_date` a

WHERE a.day NOT IN (SELECT DISTINCT cohort_date FROM second_retention_table)

AND a.day BETWEEN (SELECT MIN(cohort_date) FROM second_retention_table) AND (SELECT MAX(cohort_date) FROM second_retention_table)



ORDER BY 1



