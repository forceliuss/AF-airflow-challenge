{{ config(materialized='view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ source('raw',
        'carts') }}
), RENAMED AS (
    SELECT
        ID AS CART_ID,
        CUSTOMER_ID,
        TOTAL::DECIMAL(10,
        2) AS CART_TOTAL,
        STATUS,
        ITEMS,
        CREATED_AT,
        UPDATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED