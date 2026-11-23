-- Gold: enriched item-level table joining items, orders, products and suppliers
CREATE OR REFRESH MATERIALIZED VIEW gold.order_items
AS SELECT
  oi.item_id,
  oi.order_id,
  o.customer_id,
  o.order_date,
  o.status,
  oi.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  oi.unit_price,
  CAST(oi.quantity * oi.unit_price AS DECIMAL(12,2)) AS total_amount,
  p.supplier_id,
  s.supplier_name,
  s.country,
  s.rating
FROM silver.stg_order_items oi
JOIN silver.stg_orders    o ON oi.order_id   = o.order_id
JOIN silver.stg_products  p ON oi.product_id = p.product_id
JOIN silver.stg_suppliers s ON p.supplier_id = s.supplier_id;
