{{ config(materialized='view') }}

WITH all_dates AS (
    SELECT DISTINCT DATE_TRUNC('day', created_at) AS date_key
    FROM POSTGRES_BATCH.BRONZE.TRANSACTION_ICEBERG

    UNION

    SELECT DISTINCT DATE_TRUNC('day', created_at) AS date_key
    FROM POSTGRES_BATCH.BRONZE.PERSON_ICEBERG

    -- Add more sources here if needed, like ORDER_ITEM_ICEBERG
)
SELECT
  date_key,
  EXTRACT(year FROM date_key) AS year,
  EXTRACT(quarter FROM date_key) AS quarter,
  EXTRACT(month FROM date_key) AS month,
  INITCAP(TO_CHAR(date_key, 'Month')) AS month_name,
  EXTRACT(day FROM date_key) AS day,
  EXTRACT(dow FROM date_key) AS day_of_week,
  INITCAP(TO_CHAR(date_key, 'Day')) AS day_name,
  CASE WHEN EXTRACT(dow FROM date_key) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
  EXTRACT(week FROM date_key) AS week_of_year,
  EXTRACT(dayofyear FROM date_key) AS day_of_year,
  TO_CHAR(date_key, 'YYYY-MM') AS year_month,
  TO_CHAR(date_key, 'YYYY') || '-Q' || EXTRACT(quarter FROM date_key) AS year_quarter
FROM all_dates