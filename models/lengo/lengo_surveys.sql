{{  config(materialized='table')     }}


WITH shops_geo_loc_table AS (

SELECT

    a.*,

    b.region,

    b.country,

    b.zone,

    b.neighborhood,

    c.quality_class

FROM data-finance-staging.dbt_clement_msika.lengo_dim_shops a

INNER JOIN {{ref('lengo_dim_geo_locations')}} b ON b.geo_location_id = a.geo_location_id

INNER JOIN {{ref('lengo_dim_quality_classes')}}  c ON c.quality_class_id = a.quality_class_id

),

survey_table AS (

SELECT

    SUBSTR(a.survey_id,6) AS survey_id,

    a.survey_group,

    a.ymd,

    DATE_TRUNC(DATE(a.ymd), MONTH) AS ym,

    product_id,

    shop_id,


    a.shop_type,

    a.channel,

    a.latitude,

    a.longitude,

    d.geo_location_id,

    g.top_of_mind,

    a.units,

    a.price,

    a.availability


FROM data-finance-staging.dbt_clement_msika.lengo_presence a

INNER JOIN {{ref('lengo_dim_geo_locations')}} d ON d.country = a.country AND d.region = a.region AND d.neighborhood = a.neighborhood AND d.zone = a.zone

INNER JOIN {{ref('lengo_dim_quality_classes')}} e ON e.quality_class = a.quality_class

INNER JOIN {{ref('lengo_dim_brands')}} b ON b.brand_name = a.brand_name AND b.category = a.category AND b.sub_category = a.category

INNER JOIN {{ref('lengo_dim_products')}} c ON c.product_name = a.product_name AND c.product_format = a.product_format AND c.format_family = a.format_family

INNER JOIN shops_geo_loc_table f ON f.region = a.region AND f.neighborhood = a.neighborhood AND f.zone = a.zone AND f.country = a.country AND f.neighborhood = a.neighborhood  AND f.shop_type = a.shop_type AND f.channel = a.channel AND f.latitude = a.latitude AND f.longitude = a.longitude AND DATE(f.ymd) = DATE(a.ymd) AND f.quality_class = a.quality_class

LEFT JOIN data-finance-staging.dbt_clement_msika.lengo_notoriety g ON SUBSTR(g.survey_id,6) = SUBSTR(a.survey_id,6) AND DATE(g.ymd) = DATE(a.ymd) AND g.original_shop_id = a.original_shop_id  

)

SELECT

    ROW_NUMBER() OVER (ORDER BY survey_id, product_id, ym) AS id,

    a.*


FROM survey_table a

ORDER BY 1
