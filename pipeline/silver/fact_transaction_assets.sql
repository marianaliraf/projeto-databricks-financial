-- ===============================
-- SILVER: fact_transaction_assets
-- Símbolo padronizado: BTC/GOLD/OIL/SILVER
-- ===============================
CREATE OR REFRESH STREAMING LIVE TABLE silver.fact_transaction_assets
(
  CONSTRAINT quantidade_positive     EXPECT (quantidade > 0) ON VIOLATION DROP ROW,
  CONSTRAINT data_hora_not_null      EXPECT (data_hora IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT tipo_operacao_valid     EXPECT (tipo_operacao IN ('COMPRA','VENDA')) ON VIOLATION DROP ROW,
  CONSTRAINT asset_symbol_valid      EXPECT (asset_symbol IN ('BTC','GOLD','OIL','SILVER')) ON VIOLATION DROP ROW
)
AS
SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP)                                        AS data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP))                    AS data_hora_aproximada,

  -- 🔁 Mapeamento unificado de símbolo
  CASE 
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('BTC','BTC-USD') THEN 'BTC'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('GOLD','GC=F')   THEN 'GOLD'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('OIL','CL=F')    THEN 'OIL'
    WHEN UPPER(COALESCE(ativo, commodity_code)) IN ('SILVER','SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END                                                                 AS asset_symbol,

  CAST(quantidade AS DECIMAL(18,8))                                   AS quantidade,
  tipo_operacao,
  UPPER(moeda)                                                        AS moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  current_timestamp()                                                 AS processed_at
FROM (
  SELECT 
    transaction_id, data_hora, ativo, NULL AS commodity_code, quantidade,
    tipo_operacao, moeda, cliente_id, canal, mercado, arquivo_origem, importado_em
  FROM STREAM(bronze.sales_btc)

  UNION ALL

  SELECT 
    transaction_id, data_hora, NULL AS ativo, commodity_code, quantidade,
    tipo_operacao, moeda, cliente_id, canal, mercado, arquivo_origem, importado_em
  FROM STREAM(bronze.sales_commodities)
) combined_transactions;