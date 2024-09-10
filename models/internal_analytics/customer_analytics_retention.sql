{{ config(materialized='table') }}

SELECT 

    'daily' AS period,
    *

FROM {{ ref('customer_analytics_retention_daily')  }}

UNION ALL

SELECT 

    'weekly' AS period,
    *

FROM {{ ref('customer_analytics_retention_weekly')  }}

UNION ALL

SELECT 

    'monthly' AS period,
    *

FROM {{ ref('customer_analytics_retention_monthly')  }}

