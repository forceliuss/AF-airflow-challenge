{{ CONFIG(MATERIALIZED='view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ SOURCE('raw',
        'logistict') }}
), RENAMED AS (
    SELECT
        ID AS LOGISTICS_ID,
        CART_ID,
        STATUS AS SHIPPING_STATUS,
        TRACKING_NUMBER,
        CARRIER,
        ESTIMATED_DELIVERY_DATE,
        ACTUAL_DELIVERY_DATE,
        SHIPPING_COST::DECIMAL(10,
        2) AS SHIPPING_COST,
        CREATED_AT,
        UPDATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED