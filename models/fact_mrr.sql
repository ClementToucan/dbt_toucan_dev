

{{config(materialized='table') }}


WITH monthly_table AS (

SELECT

    name AS opportunity_name,

    id AS opportunity_id,

    account_name,

    account_id,

    date_month,

    total_arr_eur AS annual_recurring_revenue

FROM data-finance-staging.prod_tctc_salesforce.recurring_revenue_monthly



GROUP BY 1,2,3,4,5,6

),

period_table AS (


SELECT

    id,

    COUNT(DISTINCT(date_month)) AS nb_months


FROM data-finance-staging.prod_tctc_salesforce.recurring_revenue_monthly


GROUP BY 1



),

mrr_table AS (

SELECT

    opportunity_name,

    opportunity_id,

    account_name,

    account_id,

    CAST(date_month || '-01' AS DATE) AS ym,

    nb_months,

    annual_recurring_revenue,

    CASE
    WHEN nb_months > 0 THEN CAST(annual_recurring_revenue AS DECIMAL) / CAST(nb_months AS DECIMAL)
    ELSE NULL
    END AS monthly_recurring_revenue

FROM monthly_table a

LEFT JOIN period_table b ON b.id = a.opportunity_id

)

SELECT

    CASE
    WHEN account_id = '0010900001C1MuuAAF' THEN 'MOBILIZE Power Solutions - Ex-Elexent'
    WHEN account_id = '00109000005fx5LAAQ' THEN 'Littoral-Normand'
    ELSE account_name
    END AS account_name,

    CASE
    WHEN account_id = '0010900001C1MuuAAF' THEN '00109000005fznBAAQ'
    WHEN account_id = '00109000005fx5LAAQ' THEN '00109000005ftOVAAY'
    ELSE account_id
    END AS  account_id,

    ym,

    SUM(monthly_recurring_revenue) AS monthly_recurring_revenue

FROM mrr_table

WHERE DATE(ym) >= DATE('2022-01-01')

GROUP BY 1,2,3

HAVING monthly_recurring_revenue > 0

ORDER BY 1,2,3
