{{
config(
  materialized='table',
)
}}


WITH all_products AS (

SELECT

DISTINCT

    product_name,

    product_format,

    format_family,

    brand_id

FROM data-finance-staging.dbt_clement_msika.lengo_presence a

INNER JOIN {{ref('lengo_dim_brands')}} b ON b.brand_name = a.brand_name

AND b.category = a.category

AND b.sub_category = a.sub_category

)

SELECT

    ROW_NUMBER() OVER (ORDER BY product_name, product_format, format_family, brand_id) AS product_id,

    product_name,

    product_format,

    format_family,

    brand_id

FROM all_products

ORDER BY 1
