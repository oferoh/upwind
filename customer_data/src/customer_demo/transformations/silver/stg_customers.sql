-- Silver: cleaned and typed customers (missing email normalized to 'unknown')
CREATE OR REFRESH MATERIALIZED VIEW silver.stg_customers
AS SELECT
  CAST(customer_id   AS INT)  AS customer_id,
  TRIM(customer_name)         AS customer_name,
  TRIM(email)                 AS email,
  COALESCE(TRIM(email), 'unknown') AS email_clean,
  TRIM(city)                  AS city,
  CAST(signup_date AS DATE)   AS signup_date
FROM bronze.raw_customers;
