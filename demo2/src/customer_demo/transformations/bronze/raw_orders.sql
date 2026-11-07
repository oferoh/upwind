-- Bronze: raw orders ingested from CSV in the raw_csv volume
CREATE OR REFRESH STREAMING TABLE raw_orders
AS SELECT
  order_id,
  customer_id,
  order_date,
  status
FROM STREAM(read_files(
  '/Volumes/ofer_demo/bronze/raw_csv/raw_orders',
  format => 'csv',
  header => true,
  inferSchema => true
));
