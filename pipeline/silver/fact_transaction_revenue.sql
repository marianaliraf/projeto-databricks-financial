CREATE OR REFRESH STREAMING TABLE lakehouse.silver.fact_transaction_revenue AS
SELECT 
  t.transaction_id, t.data_hora, t.data_hora_aproximada, t.asset_symbol,
  t.quantidade, t.tipo_operacao, t.moeda, t.cliente_id, t.canal, t.mercado,
  c.customer_id                  AS customer_sk,
  q.preco                        AS preco_cotacao,
  q.horario_coleta               AS timestamp_cotacao,
  q.data_hora_aproximada         AS cotacao_hora_aproximada,
  (t.quantidade * q.preco)       AS gross_value,
  CASE WHEN t.tipo_operacao='VENDA'  THEN  (t.quantidade * q.preco)
       WHEN t.tipo_operacao='COMPRA' THEN -(t.quantidade * q.preco) END AS gross_value_sinal,
  (t.quantidade * q.preco * 0.0025) AS fee_revenue,
  t.processed_at, current_timestamp() AS calculated_at
FROM STREAM(lakehouse.silver.fact_transaction_assets) t
JOIN       lakehouse.silver.dim_clientes       c ON t.cliente_id = c.customer_id
JOIN       lakehouse.silver.quotation_per_hour q ON q.ativo = t.asset_symbol
                                                 AND q.data_hora_aproximada = t.data_hora_aproximada;