{{ config(materialized='table')  }}



WITH user_table AS (


SELECT

    distinct_id AS user_id,

    CASE
    WHEN toucan_version = 'Unknown' THEN 'V2'
    WHEN toucan_version = 'V3' THEN 'V3'
    WHEN toucan_version = 'LTS' THEN 'V2'
    ELSE 'V2'
    END AS toucan_version,

    time,

    CASE
      WHEN mp_event_name = 'Story Opened' THEN 'Stories'
      WHEN mp_event_name = 'Home Opened' THEN 'Homepage'
      WHEN mp_event_name = 'Execsum Preview' THEN 'PDF Report'
      WHEN mp_event_name = 'Home preview' THEN 'Send Home by Mail'
      WHEN mp_event_name = 'Personal report viewed' THEN 'My Favorites'
      WHEN mp_event_name = 'Data Wall Displayed' THEN 'Datawall'
      WHEN mp_event_name = 'Report sent by email' THEN 'Stories shared by Mail'
      ELSE 'Unknown Event'
    END AS interactions,

    a.smallappname AS applications,

    a.instance AS client



FROM mixpanel.raw__mp_master_event a

LEFT JOIN {{ref('dim_instances_over_time')}} b ON b.instance = a.instance AND b.ym = DATE_TRUNC(DATE(a.time), MONTH)


WHERE DATE(time) >= DATE(CURRENT_DATE) - INTERVAL '12' MONTH 

AND b.instance_usage = 'clients'



AND role = 'USER'
AND privilege = 'view'
AND a.instance IS NOT NULL
AND smallappname IS NOT NULL
AND mp_event_name IN ('Story Opened', 'Home Opened', 'Execsum Preview', 'Home preview', 'Personal report viewed', 'Data Wall Displayed', 'Report sent by email')


), 


cohort_table AS (

SELECT

    user_id,

    applications,

    client,

    toucan_version,

    interactions,

    MIN(DATE(time)) AS cohort_date 


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL


SELECT

    user_id,

    applications,

    client,

    toucan_version,

    'All' AS interactions,

    MIN(DATE(time)) AS cohort_date 


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL


SELECT

    user_id,

    'All' AS applications,

    client,

    toucan_version,

    interactions,

    MIN(DATE(time)) AS cohort_date 


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL 

SELECT

    user_id,

    'All' AS applications,

    client,

    toucan_version,

    'All' AS interactions,

    MIN(DATE(time)) AS cohort_date 


FROM user_table

GROUP BY 1,2,3,4,5


),

activity_table AS (

SELECT

    user_id,

    interactions,

    toucan_version,


    DATE(time) AS activity_date,

    applications,

    client

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    'All' AS interactions,

    toucan_version,


    DATE(time) AS activity_date,

    applications,

    client

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    interactions,

    toucan_version,


    DATE(time) AS activity_date,

    'All' AS applications,

    client

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    'All' AS interactions,

    toucan_version,

    DATE(time) AS activity_date,

    'All' AS applications,

    client

FROM user_table

GROUP BY 1,2,3,4,5,6

), 

combination_table AS (

SELECT

DISTINCT


    interactions,

    toucan_version,

    applications,

    client,

    user_id


FROM activity_table

),

date_table AS (


SELECT

    DATE(a.day) AS ymd,

    b.toucan_version,

    b.applications,

    b.client,

    b.interactions,

    b.user_id

FROM `utils.dim_date` a

CROSS JOIN combination_table b 

WHERE DATE(day) BETWEEN (SELECT MIN(cohort_date) FROM cohort_table) AND (SELECT MAX(cohort_date) FROM cohort_table)

ORDER BY 1,2,3,4,5

),

first_user_activity_table AS (

SELECT

    a.ymd,

    a.user_id,

    b.cohort_date,

    activity_date,

    a.interactions,

    a.applications,

    a.client,

    a.toucan_version,

    LAG(activity_date, 1, NULL)  OVER (PARTITION BY c.user_id, c.interactions, c.applications, c.client, c.toucan_version ORDER BY a.ymd) AS previous_activity_date


FROM cohort_table b


LEFT JOIN date_table a ON b.user_id = a.user_id AND b.interactions = a.interactions AND b.applications = a.applications AND b.client = a.client AND b.toucan_version = a.toucan_version

LEFT JOIN activity_table c ON c.activity_date = a.ymd AND a.user_id = c.user_id AND a.interactions = c.interactions AND a.applications = c.applications AND a.client = c.client AND a.toucan_version = c.toucan_version

WHERE a.ymd >= DATE(b.cohort_date)


),

second_user_activity_table AS (


SELECT

    ymd,

    user_id,

    cohort_date,

    activity_date,

    interactions,

    applications,

    client,

    toucan_version,

    previous_activity_date

    


FROM first_user_activity_table

),

third_user_activity_table AS  (

SELECT



    *,

    SUM(CASE WHEN activity_date IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, interactions, applications, client, toucan_version ORDER BY ymd) AS counter

FROM second_user_activity_table


ORDER BY 1,2,5,6,7,8

),

last_activity_date_table AS (

SELECT
 
    user_id,

    cohort_date,

    interactions,

    applications,

    client,

    toucan_version,

    counter,
    
    MIN(activity_date) AS previous_activity_date_bis

FROM third_user_activity_table

GROUP BY 1,2,3,4,5,6,7


),

fourth_user_activity_table AS (

SELECT

    a.ymd,

    a.user_id,

    a.cohort_date,

    a.activity_date,

    a.interactions,

    a.applications,

    a.client,

    a.toucan_version,


    CASE
    WHEN previous_activity_date IS NULL THEN previous_activity_date_bis
    ELSE previous_activity_date
    END AS previous_activity_date





FROM third_user_activity_table a

LEFT JOIN last_activity_date_table b  ON a.user_id = b.user_id AND a.interactions = b.interactions AND a.applications = b.applications AND a.client = b.client AND a.toucan_version = b.toucan_version AND a.counter = b.counter

ORDER BY 1,2,5,6,7,8

),

fifth_user_activity_table AS (

SELECT

    a.*,

    CASE
    WHEN DATE(cohort_date) = DATE(activity_date) THEN 'new'
    WHEN DATE_DIFF(activity_date, previous_activity_date, DAY) = 1 AND activity_date IS NOT NULL THEN 'retained'
    WHEN DATE_DIFF(ymd, previous_activity_date, DAY) >= 1 AND activity_date IS NULL THEN 'dormant'
    WHEN  DATE_DIFF(activity_date, previous_activity_date, DAY) > 1 AND activity_date IS NOT NULL THEN 'resurrected'
    ELSE 'other'
    END AS cohort





FROM fourth_user_activity_table a

-- MixPanel reference -- 


)


SELECT

   ymd AS date,

   FORMAT_DATE('%B %d %Y', ymd) AS period_label,

   interactions,

   applications,

   client,

   toucan_version,

   cohort,

   COUNT(DISTINCT(user_id)) AS number_users

FROM fifth_user_activity_table


GROUP BY 1,2,3,4,5,6,7

ORDER BY 1,2,3,4,5,6,7


