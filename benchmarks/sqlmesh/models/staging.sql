MODEL (
  name benchmark.staging_profiles,
  kind FULL
);

SELECT 
    profile_id,
    email,
    email_status,
    address,
    preferences
FROM read_parquet(@profiles_path);