{{ CONFIG(MATERIALIZED='view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ SOURCE('raw',
        'products') }}
)
SELECT
    ID             AS PRODUCT_ID,
    NAME           AS PRODUCT_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    STOCK_QUANTITY,
    CREATED_AT,
    UPDATED_AT
FROM
    SOURCE