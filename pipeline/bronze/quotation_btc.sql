CREATE OR REFRESH STREAMING LIVE TABLE bronze.bitcoin
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot/',
  'json',
  map(
    'cloudFiles.inferColumnTypes','true',
    'cloudFiles.schemaEvolutionMode','addNewColumns',
    'cloudFiles.schemaLocation','/Volumes/lakehouse/bronze/infra/autoloader/schemas/bitcoin',
    'checkpointLocation','/Volumes/lakehouse/bronze/infra/autoloader/checkpoints/bitcoin'
  )
);

