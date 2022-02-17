{{ config(materialized='view') }}

select * from {{ source('staging', 'green_tripdata_external_table') }}
limit 100