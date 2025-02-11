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
        (ITEM->>'quantity')::INTEGER AS QUANTITY,
        (ITEM->>'price')::DECIMAL(10,
        2) AS UNIT_PRICE,
        (ITEM->>'quantity')::INTEGER * (ITEM->>'price')::DECIMAL(10,
        2) AS LINE_TOTAL
    FROM
        CART_ITEMS
)
SELECT
    *
FROM
    EXPANDED