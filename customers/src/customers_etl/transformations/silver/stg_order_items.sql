-- Silver: cleaned and typed order items
CREATE OR REFRESH MATERIALIZED VIEW ofer_demo.silver.stg_order_items
AS SELECT
  CAST(item_id    AS INT)              AS item_id,
  CAST(order_id   AS INT)              AS order_id,
  CAST(product_id AS INT)              AS product_id,
  CAST(quantity   AS INT)              AS quantity,
  CAST(unit_price AS DECIMAL(10,2))    AS unit_price
FROM ofer_demo.bronze.raw_items;
