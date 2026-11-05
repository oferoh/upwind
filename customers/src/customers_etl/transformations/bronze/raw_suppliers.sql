-- Bronze: raw suppliers ingested from CSV in the raw_csv volume
CREATE OR REFRESH STREAMING TABLE ofer_demo.bronze.raw_suppliers
AS SELECT
  supplier_id,
  supplier_name,
  country,
  rating
FROM STREAM(read_files(
  '/Volumes/ofer_demo/bronze/raw_csv/raw_suppliers',
  format => 'csv',
  header => true,
  inferSchema => true
));
