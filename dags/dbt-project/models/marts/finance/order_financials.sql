{{ CONFIG(MATERIALIZED='table') }}

WITH ORDER_DETAILS AS (
    SELECT
        CA.CART_ID,
        CA.CUSTOMER_ID,
        CA.CART_TOTAL                   AS ORDER_TOTAL,
        L.SHIPPING_COST,
        CA.CART_TOTAL + L.SHIPPING_COST AS TOTAL_WITH_SHIPPING,
        CA.CREATED_AT                   AS ORDER_DATE,
        L.ACTUAL_DELIVERY_DATE          AS DELIVERY_DATE,
        L.SHIPPING_STATUS,
        CA.STATUS                       AS ORDER_STATUS
    FROM
        {{ REF('stg_carts') }} CA
        LEFT JOIN {{ REF('stg_logistics') }} L
        ON CA.CART_ID = L.CART_ID
), ORDER_ITEMS_DETAIL AS (
    SELECT
        CI.CART_ID,
        SUM(CI.QUANTITY * P.PRODUCT_PRICE)                   AS TOTAL_COST_PRICE,
        SUM(CI.LINE_TOTAL)                                   AS TOTAL_SELLING_PRICE,
        SUM(CI.LINE_TOTAL - (CI.QUANTITY * P.PRODUCT_PRICE)) AS GROSS_PROFIT
    FROM
        {{ REF('int_cart_items') }} CI
        LEFT JOIN {{ REF('stg_products') }} P
        ON CI.PRODUCT_ID = P.PRODUCT_ID
    GROUP BY
        1
)
SELECT
    OD.*,
    OID.TOTAL_COST_PRICE,
    OID.TOTAL_SELLING_PRICE,
    OID.GROSS_PROFIT,
    OD.SHIPPING_COST                    AS LOGISTICS_COST,
    OID.GROSS_PROFIT - OD.SHIPPING_COST AS NET_PROFIT,
    CASE
        WHEN OD.TOTAL_WITH_SHIPPING > 0
        THEN
            ((OID.GROSS_PROFIT - OD.SHIPPING_COST) / OD.TOTAL_WITH_SHIPPING) * 100
        ELSE
            0
    END                                 AS PROFIT_MARGIN_PERCENTAGE
FROM
    ORDER_DETAILS      OD
    LEFT JOIN ORDER_ITEMS_DETAIL OID
    ON OD.CART_ID = OID.CART_ID