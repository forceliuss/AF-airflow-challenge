{{ config(materialized='view') }}

WITH SOURCE AS (
    SELECT
        *
    FROM
        {{ source('raw',
        'customer') }}
), RENAMED AS (
    SELECT
        ID          AS CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        ADDRESS,
        CITY,
        STATE,
        COUNTRY,
        POSTAL_CODE,
        CREATED_AT,
        UPDATED_AT
    FROM
        SOURCE
)
SELECT
    *
FROM
    RENAMED