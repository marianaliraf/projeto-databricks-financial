-- Silver Layer: fact_quotation_assets
-- União de cotações BTC e yFinance com padronização de ativo, preço e moeda

CREATE OR REFRESH STREAMING TABLE silver.fact_quotation_assets(
  CONSTRAINT preco_positive EXPECT (preco > 0) ON VIOLATION DROP ROW,
  CONSTRAINT horario_coleta_valid EXPECT (horario_coleta <= current_timestamp()) ON VIOLATION DROP ROW,
  CONSTRAINT ativo_valid EXPECT (ativo IS NOT NULL AND ativo != '') ON VIOLATION DROP ROW,
  CONSTRAINT moeda_valid EXPECT (moeda = 'USD') ON VIOLATION DROP ROW
) AS SELECT 
  CASE 
    WHEN UPPER(ativo) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GOLD','GC=F')   THEN 'GOLD'
    WHEN UPPER(ativo) IN ('OIL','CL=F')    THEN 'OIL'
    WHEN UPPER(ativo) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END    ativo,
  CAST(preco AS DECIMAL(18,4)) as preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) as horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  current_timestamp() as processed_at
FROM (
  -- Cotações Bitcoin
  SELECT 
    ativo,
    preco,
    moeda,
    horario_coleta
  FROM STREAM(bronze.bitcoin)
  
  UNION ALL
  
  -- Cotações yFinance
  SELECT 
    ativo,
    preco,
    moeda,
    horario_coleta
  FROM STREAM(bronze.yfinance)
) combined_quotations