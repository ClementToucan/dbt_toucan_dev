{{

config(

materialized = 'table'

)


}}

WITH all_brands_table AS (

SELECT

DISTINCT

    brand_name,

    category,

    sub_category

FROM data-finance-staging.dbt_clement_msika.lengo_presence

)

SELECT

    ROW_NUMBER() OVER (ORDER BY brand_name, category, sub_category) AS brand_id,

    brand_name,

    category,

    sub_category

FROM all_brands_table


