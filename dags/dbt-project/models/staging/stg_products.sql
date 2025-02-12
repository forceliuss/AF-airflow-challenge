{{ config(materialized = 'view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ source('raw', 'raw_products') }}
)
SELECT
    ID AS PRODUCT_ID,
    NAME AS PRODUCT_NAME,
    CATEGORY,
    PRICE,
    CREATED_AT
FROM
    SOURCE