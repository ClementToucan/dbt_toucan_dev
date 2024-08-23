{{config(materialized='table')}}


WITH user_table AS (


SELECT

    distinct_id AS user_id,

    time,

    CASE
      WHEN mp_event_name = 'Story Opened' THEN 'Stories'
      WHEN mp_event_name IN ('Small App report', 'Small App', 'Small App Opened', 'Small App slide') THEN 'Small App'
      WHEN mp_event_name = 'Error message' THEN 'Error'
      WHEN mp_event_name = 'Instance Login' THEN 'Login'
      WHEN mp_event_name = 'Home Opened' THEN 'Homepage'
      WHEN mp_event_name = 'Execsum Preview' THEN 'Executive Summary Preview'
      WHEN mp_event_name = 'Exploration filter loaded' THEN 'Exploration Filter'
      WHEN mp_event_name = 'TableChart displayed' THEN 'Table Chart'
      WHEN mp_event_name = 'Logout' THEN 'Logout'
      WHEN mp_event_name IN ('Smart editor', 'Settings', 'Additional Panel', 'Dataset configuration') THEN mp_event_name
      ELSE 'Other Events'
    END AS interactions


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

    interactions,


    DATE(time) AS activity_date 

FROM user_table

GROUP BY 1,2,3



),

date_table AS (


SELECT

    DATE(day) AS ymd

FROM `utils.dim_date`

WHERE DATE(day) BETWEEN (SELECT MIN(cohort_date) FROM cohort_table) AND (SELECT MAX(cohort_date) FROM cohort_table)



),

first_user_activity_table AS (

SELECT

    a.ymd,

    b.user_id,

    b.cohort_date,

    activity_date,

    interactions,

    LAG(activity_date) OVER (PARTITION BY c.user_id, interactions ORDER BY activity_date) AS previous_activity_date




FROM date_table a 

LEFT JOIN activity_table c ON c.activity_date = a.ymd

LEFT JOIN cohort_table b ON b.user_id = c.user_id

WHERE interactions IS NOT NULL

)


SELECT

    FORMAT_DATE('%Y', DATE(ymd)) AS y,

    FORMAT_DATE('%m', DATE(ymd)) AS month_number,

    interactions,

    COUNT(DISTINCT(CASE WHEN DATE(cohort_date) = DATE(activity_date) THEN user_id ELSE NULL END)) AS new_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) <= 7 THEN user_id ELSE NULL END)) AS current_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) BETWEEN 8 AND 30 THEN user_id ELSE NULL END)) AS reactivated_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) > 30 THEN user_id ELSE NULL END)) AS resurrected_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) <= 7 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS at_risk_weekly_active_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) BETWEEN 8 AND 30 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS at_risk_monthly_active_users,

    COUNT(DISTINCT(CASE WHEN DATE_DIFF(ymd, previous_activity_date, DAY) > 30 AND activity_date IS NULL THEN user_id ELSE NULL END)) AS dormant_users,

    COUNT(DISTINCT(user_id)) AS active_users




    

FROM first_user_activity_table

GROUP BY 1,2,3

ORDER BY 1,2,3
