CREATE OR REFRESH STREAMING LIVE TABLE bitcoin
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot/',  -- 📥 origem dos JSONs
  'json',
  map(
    -- Processa apenas novos arquivos (se quiser processar os antigos, use 'true')
    'cloudFiles.includeExistingFiles', 'false',

    -- Detecta automaticamente os tipos de colunas no JSON
    'cloudFiles.inferColumnTypes', 'true',

    -- Evolui o schema automaticamente quando chegam novos campos
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',

    -- 🧭 Local seguro (Volume UC) para salvar schema inferido
    'cloudFiles.schemaLocation', '/Volumes/lakehouse/bronze/infra/autoloader/schemas/bitcoin',

    -- 🧾 Local seguro para checkpoints do streaming
    'checkpointLocation', '/Volumes/lakehouse/bronze/infra/autoloader/checkpoints/bitcoin'
  )
);
