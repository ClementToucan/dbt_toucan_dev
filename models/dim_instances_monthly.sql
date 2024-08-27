{{ config(materialized='table')   }}


WITH first_table AS (

SELECT 

    name AS instance_name,

    frontend_vhost,

    extracted_date_month,

    last_updated_source,

    toucan_version,

    disabled,

    account_type,

    account_id,

    ROW_NUMBER () OVER (PARTITION BY frontend_vhost, extracted_date_month ORDER BY name, vhost_server_name) AS idx



FROM `data-finance-staging.prod_config_analytics.shaun_instances_over_time`

WHERE instance_type = 'Customers'



),

-- 1 instance and frontend_vhost per month

second_table AS (

SELECT

    a.instance_name,

    a.frontend_vhost,

    a.extracted_date_month,

    a.last_updated_source,

    a.toucan_version,

    a.disabled,

    a.account_type,

    a.account_id,

    ROW_NUMBER () OVER (PARTITION BY a.instance_name, a.frontend_vhost  ORDER BY a.instance_name) AS idx_second

FROM first_table a


WHERE idx = 1



),

latest_shaun_version_table AS (

SELECT 

  

    instance_name,

    frontend_vhost,


    MAX(extracted_date_month) AS last_extraction_month

    



FROM second_table


WHERE account_id IS NOT NULL

GROUP BY 1,2



),

last_account_per_url AS (

SELECT

    b.instance_name,

    b.frontend_vhost,

    b.account_id



FROM latest_shaun_version_table a

INNER JOIN second_table b ON a.instance_name = b.instance_name AND a.frontend_vhost = b.frontend_vhost AND b.extracted_date_month = a.last_extraction_month



)

SELECT

    a.instance_name,

    a.frontend_vhost,

    a.extracted_date_month,

    a.toucan_version,

    a.disabled,

    b.account_id


FROM second_table a

LEFT JOIN last_account_per_url b ON b.instance_name = a.instance_name AND a.frontend_vhost = b.frontend_vhost

