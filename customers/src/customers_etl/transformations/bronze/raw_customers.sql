-- Bronze: raw customers ingested from CSV in the raw_csv volume
CREATE OR REFRESH STREAMING TABLE ofer_demo.bronze.raw_customers
AS SELECT
  customer_id,
  customer_name,
  email,
  city,
  signup_date
FROM STREAM(read_files(
  '/Volumes/ofer_demo/bronze/raw_csv/raw_customers',
  format => 'csv',
  header => true,
  inferSchema => true
));
