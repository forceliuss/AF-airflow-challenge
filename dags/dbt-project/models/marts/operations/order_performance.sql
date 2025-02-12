{{ CONFIG(MATERIALIZED='table') }}

WITH ORDER_LOGISTICS AS (
    SELECT
        L.CART_ID,
        L.SHIPPING_METHOD,
        L.SHIPPING_STATUS,
        L.ESTIMATED_DELIVERY_AT,
        L.DELIVERED_AT,
        CASE
            WHEN L.DELIVERED_AT IS NOT NULL AND L.ESTIMATED_DELIVERY_AT IS NOT NULL
            THEN
                CASE
                    WHEN L.DELIVERED_AT <= L.ESTIMATED_DELIVERY_AT THEN
                        1
                    ELSE
                        0
                END
            ELSE
                NULL
        END AS ON_TIME_DELIVERY,
        CASE
            WHEN L.DELIVERED_AT IS NOT NULL AND L.ESTIMATED_DELIVERY_AT IS NOT NULL
            THEN
                EXTRACT(DAY FROM L.DELIVERED_AT - L.ESTIMATED_DELIVERY_AT)
            ELSE
                NULL
        END AS DELIVERY_DELAY_DAYS
    FROM
        {{ REF('stg_logistics') }} L
), PRODUCT_PERFORMANCE AS (
    SELECT
        CI.CART_ID,
        COUNT(DISTINCT CI.PRODUCT_ID) AS UNIQUE_PRODUCTS_PER_ORDER,
        SUM(CI.QUANTITY) AS TOTAL_ITEMS_PER_ORDER,
        AVG(P.STOCK_QUANTITY) AS AVG_STOCK_LEVEL_AT_ORDER,
        SUM(
            CASE
                WHEN P.STOCK_QUANTITY <= 0 THEN
                    1
                ELSE
                    0
            END) AS OUT_OF_STOCK_ITEMS,
        SUM(
            CASE
                WHEN P.STOCK_QUANTITY BETWEEN 1 AND 10 THEN
                    1
                ELSE
                    0
            END) AS LOW_STOCK_ITEMS
    FROM
        {{ REF('int_cart_items') }} CI
        LEFT JOIN {{ REF('stg_products') }} P
        ON CI.PRODUCT_ID = P.PRODUCT_ID
    GROUP BY
        CI.CART_ID
), ORDER_DETAILS AS (
    SELECT
        C.CART_ID,
        C.CUSTOMER_ID,
        C.STATUS AS ORDER_STATUS,
        C.CREATED_AT AS ORDER_DATE,
        C.CART_TOTAL AS ORDER_VALUE
    FROM
        {{ REF('stg_carts') }} C
)
SELECT
    OD.*,
    PP.UNIQUE_PRODUCTS_PER_ORDER,
    PP.TOTAL_ITEMS_PER_ORDER,
    PP.AVG_STOCK_LEVEL_AT_ORDER,
    PP.OUT_OF_STOCK_ITEMS,
    PP.LOW_STOCK_ITEMS,
    OL.SHIPPING_METHOD,
    OL.SHIPPING_STATUS,
    OL.ON_TIME_DELIVERY,
    OL.DELIVERY_DELAY_DAYS,
    (OD.ORDER_VALUE / NULLIF(PP.TOTAL_ITEMS_PER_ORDER,
    0))::DECIMAL(10,
    2) AS AVG_ITEM_VALUE,
    CASE
        WHEN OL.ON_TIME_DELIVERY = 1 THEN
            'On Time'
        WHEN OL.DELIVERY_DELAY_DAYS > 0 THEN
            'Delayed'
        ELSE
            'In Transit'
    END AS DELIVERY_PERFORMANCE
FROM
    ORDER_DETAILS OD
    LEFT JOIN PRODUCT_PERFORMANCE PP
    ON OD.CART_ID = PP.CART_ID
    LEFT JOIN ORDER_LOGISTICS OL
    ON OD.CART_ID = OL.CART_ID