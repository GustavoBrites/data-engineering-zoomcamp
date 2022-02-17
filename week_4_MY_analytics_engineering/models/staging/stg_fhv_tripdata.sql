{{ config(materialized='view') }}

select * from {{ source('staging', 'fhv_tripdata_external_table') }}
limit 100