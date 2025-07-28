{{ config(materialized='table') }}

SELECT 
    profile_id,
    email,
    email_status,
    address,
    preferences
FROM read_parquet('{{ var("profiles_path") }}')