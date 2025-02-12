{{ config(materialized = 'view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ source('raw', 'raw_carts') }}
), RENAMED AS (
    SELECT
        ID AS CART_ID,
        CUSTOMER_ID,
        TOTAL_AMOUNT::DECIMAL(10, 2) AS CART_TOTAL,
        STATUS,
        ITEMS,
        SHIPPING_INFO,
        PAYMENT_INFO,
        SALE_DATE,
        CREATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED