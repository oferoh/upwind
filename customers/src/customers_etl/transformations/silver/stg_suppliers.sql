-- Silver: cleaned and typed suppliers
CREATE OR REFRESH MATERIALIZED VIEW ofer_demo.silver.stg_suppliers
AS SELECT
  CAST(supplier_id   AS INT)          AS supplier_id,
  TRIM(supplier_name)                 AS supplier_name,
  TRIM(country)                       AS country,
  CAST(rating AS DECIMAL(3,1))        AS rating
FROM ofer_demo.bronze.raw_suppliers;
