CREATE OR REFRESH LIVE TABLE silver.quotation_per_hour AS
WITH base AS (
  SELECT
    ativo,
    date_trunc('hour', to_utc_timestamp(CAST(horario_coleta AS TIMESTAMP), 'UTC')) AS data_hora_aproximada,
    max_by(preco, horario_coleta) AS preco,
    max(horario_coleta)          AS horario_coleta
  FROM silver.fact_quotation_assets      
  GROUP BY 1,2
),
limites AS (
  SELECT min(data_hora_aproximada) AS min_dt, max(data_hora_aproximada) AS max_dt FROM base
),
horas AS (
  SELECT explode(sequence((SELECT min_dt FROM limites), (SELECT max_dt FROM limites), interval 1 hour)) AS data_hora_aproximada
),
grid AS (
  SELECT a.ativo, h.data_hora_aproximada
  FROM (SELECT DISTINCT ativo FROM base) a
  CROSS JOIN horas h
),
joined AS (
  SELECT g.ativo, g.data_hora_aproximada, b.preco, b.horario_coleta
  FROM grid g
  LEFT JOIN base b
    ON b.ativo = g.ativo AND b.data_hora_aproximada = g.data_hora_aproximada
),
ff AS (
  SELECT
    ativo,
    data_hora_aproximada,
    last_value(preco, true) OVER (PARTITION BY ativo ORDER BY data_hora_aproximada
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS preco,
    last_value(horario_coleta, true) OVER (PARTITION BY ativo ORDER BY data_hora_aproximada
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS horario_coleta
  FROM joined
)
SELECT * FROM ff;
