-- Silver: cleaned and typed orders
CREATE OR REFRESH MATERIALIZED VIEW silver.stg_orders
AS SELECT
  CAST(order_id    AS INT)  AS order_id,
  CAST(customer_id AS INT)  AS customer_id,
  CAST(order_date  AS DATE) AS order_date,
  TRIM(status)              AS status
FROM bronze.raw_orders;
