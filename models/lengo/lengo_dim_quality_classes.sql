{{ config(materialized='table')  }}


WITH all_quality_classes AS (


SELECT

DISTINCT

    quality_class

FROM data-finance-staging.dbt_clement_msika.lengo_presence


)

SELECT

    ROW_NUMBER() OVER (ORDER BY quality_class) AS quality_class_id,

    quality_class

FROM all_quality_classes


ORDER BY 1

