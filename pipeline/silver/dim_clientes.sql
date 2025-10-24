-- Silver Layer: dim_clientes
-- Cópia anonimizada da base de clientes com validação de categorias

CREATE OR REFRESH STREAMING TABLE silver.dim_clientes(
  CONSTRAINT customer_id_unique EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT segmento_valid EXPECT (segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')) ON VIOLATION DROP ROW,
  CONSTRAINT pais_valid EXPECT (pais IN ('Brasil', 'Alemanha', 'Estados Unidos')) ON VIOLATION DROP ROW,
  CONSTRAINT estado_valid EXPECT (
    (pais = 'Brasil' AND estado IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'))
    OR (pais = 'Alemanha' AND estado = 'DE')
    OR (pais = 'Estados Unidos' AND estado = 'US')
  ) ON VIOLATION DROP ROW,
  CONSTRAINT documento_hash_not_null EXPECT (documento_hash IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT 
  customer_id,
  customer_name,
  SHA2(documento, 256) as documento_hash,
  segmento,
  pais,
  estado,
  cidade,
  created_at,
  current_timestamp() as processed_at
FROM STREAM(bronze.customers)