{{ config(materialized = 'table') }}

WITH CUSTOMER_ORDERS AS (
    SELECT
        C.CUSTOMER_ID,
        C.FULL_NAME,
        C.EMAIL,
        C.CITY,
        COUNT(DISTINCT CA.CART_ID) AS TOTAL_ORDERS,
        SUM(CA.CART_TOTAL) AS TOTAL_SPENT,
        AVG(CA.CART_TOTAL) AS AVG_ORDER_VALUE,
        MIN(CA.CREATED_AT) AS FIRST_ORDER_DATE,
        MAX(CA.CREATED_AT) AS LAST_ORDER_DATE
    FROM
        {{ ref('stg_customers') }} C
        LEFT JOIN {{ ref('stg_carts') }} CA
        ON C.CUSTOMER_ID = CA.CUSTOMER_ID
    WHERE
        CA.STATUS = 'completed'
    GROUP BY
        1, 2, 3, 4
), ORDER_ITEMS AS (
    SELECT
        C.CUSTOMER_ID,
        COUNT(DISTINCT CI.PRODUCT_ID) AS UNIQUE_PRODUCTS_BOUGHT,
        SUM(CI.QUANTITY) AS TOTAL_ITEMS_BOUGHT
    FROM
        {{ ref('stg_customers') }} C
        LEFT JOIN {{ ref('stg_carts') }} CA
        ON C.CUSTOMER_ID = CA.CUSTOMER_ID
        LEFT JOIN {{ ref('int_cart_items') }} CI
        ON CA.CART_ID = CI.CART_ID
    WHERE
        CA.STATUS = 'completed'
    GROUP BY
        1
)
SELECT
    CO.*,
    OI.UNIQUE_PRODUCTS_BOUGHT,
    OI.TOTAL_ITEMS_BOUGHT,
    CO.TOTAL_SPENT / NULLIF(OI.TOTAL_ITEMS_BOUGHT, 0) AS AVG_ITEM_PRICE
FROM
    CUSTOMER_ORDERS CO
    LEFT JOIN ORDER_ITEMS OI
    ON CO.CUSTOMER_ID = OI.CUSTOMER_ID