{{ config(materialized = 'view') }}

WITH SOURCE AS (
    SELECT *
    FROM {{ source('raw', 'raw_logistict') }}
),
RENAMED AS (
    SELECT 
        ID AS LOGISTICS_ID,
        COMPANY_NAME,
        SERVICE_TYPE,
        ORIGIN_WAREHOUSE,
        CONTACT_PHONE,
        CREATED_AT
    FROM SOURCE
)
SELECT *
FROM RENAMED