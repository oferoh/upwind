-- Gold: one row per order with item count and total amount
CREATE OR REFRESH MATERIALIZED VIEW ofer_demo.gold.orders
AS SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.status,
  COUNT(oi.item_id)                            AS item_count,
  CAST(COALESCE(SUM(oi.total_amount), 0) AS DECIMAL(12,2)) AS total_amount
FROM ofer_demo.silver.stg_orders o
LEFT JOIN ofer_demo.gold.order_items oi
  ON o.order_id = oi.order_id
GROUP BY ALL;
