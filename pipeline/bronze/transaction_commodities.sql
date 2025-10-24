CREATE OR REFRESH STREAMING LIVE TABLE sales_commodities
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM STREAM(lakehouse.databricks_financial_raw.sales_commodities);