{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM RAW.GLOBALMART.CUSTOMERS