{{  config(materialized='table') }}

WITH all_geo_locations AS (


SELECT

DISTINCT


    country,

    region,

    zone,

    neighborhood

FROM data-finance-staging.dbt_clement_msika.lengo_presence

)

SELECT


    ROW_NUMBER() OVER (ORDER BY country, region, zone, neighborhood) AS geo_location_id,

    country,

    region,

    zone,

    neighborhood

FROM all_geo_locations

ORDER BY 1,2,3,4



