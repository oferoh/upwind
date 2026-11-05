-- Bronze: raw products ingested from CSV in the raw_csv volume
CREATE OR REFRESH STREAMING TABLE ofer_demo.bronze.raw_products
AS SELECT
  product_id,
  product_name,
  category,
  price,
  supplier_id
FROM STREAM(read_files(
  '/Volumes/ofer_demo/bronze/raw_csv/raw_products',
  format => 'csv',
  header => true,
  inferSchema => true
));
