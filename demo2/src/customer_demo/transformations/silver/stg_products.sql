-- Silver: cleaned and typed products
CREATE OR REFRESH MATERIALIZED VIEW silver.stg_products
AS SELECT
  CAST(product_id   AS INT)           AS product_id,
  TRIM(product_name)                  AS product_name,
  TRIM(category)                      AS category,
  CAST(price AS DECIMAL(10,2))        AS price,
  CAST(supplier_id AS INT)            AS supplier_id
FROM bronze.raw_products;
