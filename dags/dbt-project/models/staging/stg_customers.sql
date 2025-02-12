{{ config(materialized = 'view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ source('raw', 'raw_customer') }}
), RENAMED AS (
    SELECT
        ID AS CUSTOMER_ID,
        FULL_NAME,
        EMAIL,
        PHONE,
        ADDRESS,
        CITY,
        CREATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED