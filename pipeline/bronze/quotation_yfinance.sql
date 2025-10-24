CREATE OR REFRESH STREAMING LIVE TABLE yfinance
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/yfinance/commodities/latest_prices/',
  'json',
  map(
    'cloudFiles.inferColumnTypes','true',
    'cloudFiles.schemaEvolutionMode','addNewColumns',
    'cloudFiles.schemaLocation','/Volumes/lakehouse/bronze/infra/autoloader/schemas/yfinance',
    'checkpointLocation','/Volumes/lakehouse/bronze/infra/autoloader/checkpoints/yfinance'
  )
);
