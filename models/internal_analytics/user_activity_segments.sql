{{ config(materialized='table')   }}

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

date_table AS (


SELECT

    DATE(day) AS ymd

FROM `utils.dim_date`

WHERE DATE(day) BETWEEN (SELECT MIN(cohort_date) FROM cohort_table) AND (SELECT MAX(cohort_date) FROM cohort_table)



),

user_activity_segment_table AS (

SELECT

    a.ymd,

    b.user_id,

    b.cohort_date,

    activity_date,

    LAG(activity_date) OVER (PARTITION BY c.user_id ORDER BY activity_date) AS previous_activity_date




FROM date_table a 

LEFT JOIN activity_table c ON c.activity_date = a.ymd

LEFT JOIN cohort_table b ON b.user_id = c.user_id

)


SELECT

    ymd,

    COUNT(DISTINCT(CASE WHEN DATE(cohort_date) = DATE(activity_date) THEN user_id ELSE NULL END)) AS new_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) <= 7 THEN user_id ELSE NULL END)) AS current_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) BETWEEN 8 AND 30 THEN user_id ELSE NULL END)) AS reactivated_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) > 30 THEN user_id ELSE NULL END)) AS resurrected_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) <= 7 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS at_risk_weekly_active_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) BETWEEN 8 AND 30 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS at_risk_monthly_active_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) > 30 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS dormant_users,

    COUNT(DISTINCT(user_id)) AS active_users




    

FROM user_activity_segment_table

GROUP BY 1

ORDER BY 1


