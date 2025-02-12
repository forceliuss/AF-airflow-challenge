{{ config(materialized='table') }}

WITH CART_ITEMS AS (
    SELECT
        CART_ID,
        JSONB_ARRAY_ELEMENTS(ITEMS::JSONB) AS ITEM
    FROM
        {{ ref('stg_carts') }}
), EXPANDED AS (
    SELECT
        CART_ID,
        (ITEM->>'product_id')::INTEGER AS PRODUCT_ID,
        (ITEM->>'product_name')::TEXT AS PRODUCT_NAME,
        (ITEM->>'quantity')::INTEGER AS QUANTITY,
        (ITEM->>'unit_price')::DECIMAL(10,
        2) AS UNIT_PRICE,
        (ITEM->>'line_total')::DECIMAL(10,
        2) AS LINE_TOTAL,
        (ITEM->>'discount')::DECIMAL(10,
        2) AS DISCOUNT
    FROM
        CART_ITEMS
)
SELECT
    *
FROM
    EXPANDED

    