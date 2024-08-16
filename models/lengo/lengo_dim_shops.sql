WITH all_shops AS (

SELECT

DISTINCT

    shop_type,

    channel,

    latitude,

    longitude,

    ymd,

    DATE_TRUNC(DATE(ymd), MONTH) AS ym,

    quality_class_id,

    geo_location_id,


FROM data-finance-staging.dbt_clement_msika.lengo_presence a

INNER JOIN {{ref('lengo_dim_geo_locations')}} d ON d.country = a.country AND d.region = a.region AND d.neighborhood = a.neighborhood AND d.zone = a.zone

INNER JOIN {{ref('lengo_dim_quality_classes')}} e ON e.quality_class = a.quality_class

)

SELECT

    ROW_NUMBER() OVER (ORDER BY shop_type, channel, latitude, longitude, ymd, quality_class_id, geo_location_id) AS shop_id,

    a.*
    

FROM all_shops a

ORDER BY 1
