{{ CONFIG(MATERIALIZED='view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ SOURCE('raw',
        'logistics') }}
), RENAMED AS (
    SELECT
        ID                    AS LOGISTICS_ID,
        CART_ID,
        STATUS,
        TRACKING_NUMBER,
        SHIPPING_METHOD,
        SHIPPING_COST,
        ESTIMATED_DELIVERY_AT,
        DELIVERED_AT,
        CREATED_AT,
        UPDATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED