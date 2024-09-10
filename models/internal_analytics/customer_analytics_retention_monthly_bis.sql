{{ config(materialized='table')}}


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


FROM data-finance-staging.mixpanel.raw__mp_master_event a


LEFT JOIN data-finance-staging.prod_customer_analytics.dim_instances_over_time b ON b.instance = a.instance AND b.ym = DATE_TRUNC(DATE(a.time), MONTH)


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

    MIN(DATE_TRUNC(DATE(time), MONTH)) AS cohort_month


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL


SELECT

    user_id,

    applications,

    client,

    toucan_version,

    'All' AS interactions,

    MIN(DATE_TRUNC(DATE(time), MONTH)) AS cohort_month


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL


SELECT

    user_id,

    'All' AS applications,

    client,

    toucan_version,

    interactions,

    MIN(DATE_TRUNC(DATE(time), MONTH)) AS cohort_month


FROM user_table

GROUP BY 1,2,3,4,5

UNION ALL 

SELECT

    user_id,

    'All' AS applications,

    client,

    toucan_version,

    'All' AS interactions,

    MIN(DATE_TRUNC(DATE(time), MONTH)) AS cohort_month


FROM user_table

GROUP BY 1,2,3,4,5


),

activity_table AS (

SELECT

    user_id,

    interactions,

    toucan_version,


    DATE_TRUNC(DATE(time), MONTH) AS activity_month,

    applications,

    client,

    COUNT(*) AS occurrences

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    'All' AS interactions,

    toucan_version,


    DATE_TRUNC(DATE(time), MONTH) AS activity_month,

    applications,

    client,

    COUNT(*) AS occurrences

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    interactions,

    toucan_version,

    DATE_TRUNC(DATE(time), MONTH) AS activity_month,


    'All' AS applications,

    client,

    COUNT(*) AS occurrences

FROM user_table

GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT

    user_id,

    'All' AS interactions,

    toucan_version,

    DATE_TRUNC(DATE(time), MONTH) AS activity_month,

    'All' AS applications,

    client,

    COUNT(*) AS occurrences

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

month_table AS (


SELECT

    DATE_TRUNC(DATE(a.day), MONTH) AS ymd,

    b.toucan_version,

    b.applications,

    b.client,

    b.interactions,

    b.user_id

FROM `utils.dim_date` a

CROSS JOIN combination_table b 

WHERE DATE(a.day) BETWEEN (SELECT MIN(cohort_month) FROM cohort_table) AND (SELECT MAX(cohort_month) FROM cohort_table)

GROUP BY 1,2,3,4,5,6

ORDER BY 1,2,3,4,5

),

first_user_activity_table AS (

SELECT

    a.ymd,

    a.user_id,

    b.cohort_month,

    activity_month,

    a.interactions,

    a.applications,

    a.client,

    a.toucan_version,

    c.occurrences,

    LAG(activity_month, 1, NULL)  OVER (PARTITION BY c.user_id, c.interactions, c.applications, c.client, c.toucan_version ORDER BY a.ymd) AS previous_activity_month


FROM cohort_table b


LEFT JOIN month_table a ON b.user_id = a.user_id AND b.interactions = a.interactions AND b.applications = a.applications AND b.client = a.client AND b.toucan_version = a.toucan_version

LEFT JOIN activity_table c ON c.activity_month = a.ymd AND a.user_id = c.user_id AND a.interactions = c.interactions AND a.applications = c.applications AND a.client = c.client AND a.toucan_version = c.toucan_version

WHERE a.ymd >= DATE(b.cohort_month)


),

second_user_activity_table AS (


SELECT

    ymd,

    user_id,

    cohort_month,

    activity_month,

    interactions,

    applications,

    client,

    toucan_version,

    previous_activity_month,

    occurrences


FROM first_user_activity_table

),

third_user_activity_table AS  (

SELECT



    *,

    SUM(CASE WHEN activity_month IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, interactions, applications, client, toucan_version ORDER BY ymd) AS counter

FROM second_user_activity_table


ORDER BY 1,2,5,6,7,8

),

last_activity_month_table AS (

SELECT
 
    user_id,

    cohort_month,

    interactions,

    applications,

    client,

    toucan_version,

    counter,
    
    MIN(activity_month) AS previous_activity_month_bis

FROM third_user_activity_table

GROUP BY 1,2,3,4,5,6,7


),

fourth_user_activity_table AS (

SELECT

    a.ymd,

    a.user_id,

    a.cohort_month,

    a.activity_month,

    a.interactions,

    a.applications,

    a.client,

    a.toucan_version,

    a.occurrences,


    CASE
    WHEN previous_activity_month IS NULL THEN previous_activity_month_bis
    ELSE previous_activity_month
    END AS previous_activity_month




FROM third_user_activity_table a

LEFT JOIN last_activity_month_table b  ON a.user_id = b.user_id AND a.interactions = b.interactions AND a.applications = b.applications AND a.client = b.client AND a.toucan_version = b.toucan_version AND a.counter = b.counter

ORDER BY 1,2,5,6,7,8

)


SELECT

    a.*,

    FORMAT_DATE('%B', ymd) AS period_label,

    CASE
    WHEN DATE(cohort_month) = DATE(activity_month) THEN 'new'
    WHEN DATE_DIFF(activity_month, previous_activity_month, MONTH) = 1 AND activity_month IS NOT NULL THEN 'retained'
    WHEN DATE_DIFF(ymd, previous_activity_month, MONTH) >= 1 AND activity_month IS NULL THEN 'dormant'
    WHEN  DATE_DIFF(activity_month, previous_activity_month, MONTH) > 1 AND activity_month IS NOT NULL THEN 'resurrected'
    ELSE 'other'
    END AS cohort,

    CASE 
      WHEN activity_month IS NOT NULL AND  interactions = 'Stories' THEN 'stories viewed'
      WHEN activity_month IS NOT NULL AND  interactions = 'Homepage' THEN 'homepages viewed'
      WHEN activity_month IS NOT NULL AND  interactions = 'PDF Report' THEN 'PDF reports viewed'
      WHEN activity_month IS NOT NULL AND  interactions = 'Send Home by Mail' THEN 'homepages sent by mail'
      WHEN activity_month IS NOT NULL AND  interactions = 'My Favorites' THEN 'My Favorites viewed'
      WHEN activity_month IS NOT NULL AND  interactions = 'Datawall' THEN 'datawalls displayed'
      WHEN activity_month IS NOT NULL AND  interactions = 'Stories shared by Mail' THEN 'stories shared by Mail'
      WHEN activity_month IS NOT NULL AND  interactions = 'All interactions' THEN 'Interactions'
      ELSE ' Not viewed'
    END AS unit





FROM fourth_user_activity_table a

-- MixPanel reference -- 





