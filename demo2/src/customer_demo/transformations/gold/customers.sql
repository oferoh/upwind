-- Gold: one row per customer with order count and total spent (cancelled orders excluded)
CREATE OR REFRESH MATERIALIZED VIEW gold.customers
AS SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.email_clean,
  c.city,
  c.signup_date,
  COUNT(o.order_id)                            AS order_count,
  CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(12,2)) AS total_spent
FROM silver.stg_customers c
LEFT JOIN gold.orders o
  ON c.customer_id = o.customer_id
 AND o.status <> 'cancelled'
GROUP BY ALL;
