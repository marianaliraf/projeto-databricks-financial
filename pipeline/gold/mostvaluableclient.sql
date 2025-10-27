-- Gold Layer: mostvaluableclient
-- Agregação por cliente: total, média, frequência, ticket médio e ranking

-- Gold (batch): pode usar janelas e ORDER BY
CREATE OR REFRESH LIVE TABLE gold.mostvaluableclient AS
WITH max_dt AS (
  SELECT max(data_hora) AS mx FROM silver.fact_transaction_revenue
),
base AS (
  SELECT
    customer_sk,
    COUNT(*)                               AS total_transacoes,
    ROUND(SUM(gross_value), 2)             AS valor_total,
    ROUND(AVG(gross_value), 2)             AS ticket_medio,
    MIN(data_hora)                         AS primeira_transacao,
    MAX(data_hora)                         AS ultima_transacao,
    SUM(CASE WHEN data_hora >= (SELECT mx FROM max_dt) - INTERVAL 30 DAYS
             THEN 1 ELSE 0 END)           AS transacoes_ultimos_30_dias,
    ROUND(SUM(fee_revenue), 2)             AS comissao_total
  FROM silver.fact_transaction_revenue
  GROUP BY customer_sk
)
SELECT
  *,
  RANK() OVER (ORDER BY total_transacoes DESC)                               AS ranking_por_transacoes,
  CASE
    WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 1 THEN 'Top 1'
    WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 2 THEN 'Top 2'
    WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 3 THEN 'Top 3'
    ELSE 'Outros'
  END AS classificacao_cliente,
  current_timestamp() AS calculated_at
FROM base;
