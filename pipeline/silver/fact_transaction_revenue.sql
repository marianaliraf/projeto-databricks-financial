-- Silver Layer: fact_transaction_revenue
-- Junção entre transações e cotações, cálculo de valor e receita de taxa (camada Silver de enriquecimento)

CREATE OR REFRESH STREAMING TABLE silver.fact_transaction_revenue(
  CONSTRAINT gross_value_positive EXPECT (gross_value > 0) ON VIOLATION DROP ROW,
  CONSTRAINT fee_revenue_positive EXPECT (fee_revenue > 0) ON VIOLATION DROP ROW,
  CONSTRAINT customer_sk_not_null EXPECT (customer_sk IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT cotacao_valida EXPECT (preco_cotacao > 0 AND timestamp_cotacao <= data_hora) ON VIOLATION DROP ROW
) AS SELECT 
  t.transaction_id,
  t.data_hora,
  t.data_hora_aproximada,
  t.asset_symbol,
  t.quantidade,
  t.tipo_operacao,
  t.moeda,
  t.cliente_id,
  t.canal,
  t.mercado,
  c.customer_id as customer_sk,
  q.preco as preco_cotacao,
  q.horario_coleta as timestamp_cotacao,
  q.data_hora_aproximada as cotacao_hora_aproximada,
  -- Cálculo do valor bruto da transação
  (t.quantidade * q.preco) as gross_value,
  -- Cálculo do valor bruto com sinal (positivo para VENDA, negativo para COMPRA)
  CASE 
    WHEN t.tipo_operacao = 'VENDA' THEN (t.quantidade * q.preco)
    WHEN t.tipo_operacao = 'COMPRA' THEN -(t.quantidade * q.preco)
    ELSE 0
  END as gross_value_sinal,
  -- Cálculo da receita de taxa (0.25%)
  (t.quantidade * q.preco * 0.0025) as fee_revenue,
  t.processed_at,
  current_timestamp() as calculated_at
FROM STREAM(silver.fact_transaction_assets) t
INNER JOIN STREAM(silver.dim_clientes) c ON t.cliente_id = c.customer_id
INNER JOIN STREAM(silver.fact_quotation_assets) q ON t.asset_symbol = q.ativo 
  AND q.data_hora_aproximada = t.data_hora_aproximada