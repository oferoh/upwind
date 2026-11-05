-- Bronze: raw order items ingested from CSV in the raw_csv volume
CREATE OR REFRESH STREAMING TABLE ofer_demo.bronze.raw_items
AS SELECT
  item_id,
  order_id,
  product_id,
  quantity,
  unit_price
FROM STREAM(read_files(
  '/Volumes/ofer_demo/bronze/raw_csv/raw_items',
  format => 'csv',
  header => true,
  inferSchema => true
));
