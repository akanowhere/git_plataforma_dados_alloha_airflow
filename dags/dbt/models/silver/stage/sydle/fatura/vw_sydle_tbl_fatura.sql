{% set catalog_schema_table=source("sydle", "fatura") %}
{% set partition_column="id_fatura" %}
{% set order_column="data_atualizacao DESC, data_extracao DESC" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_fatura,
    id_cliente,
    codigo,
    codigo_externo,
    cliente,
    tipo_documento_cliente,
    documento_cliente,
    mes_referencia,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    TRY_CAST(data_vencimento AS TIMESTAMP) AS data_vencimento,
    TRY_CAST(data_pagamento AS TIMESTAMP) AS data_pagamento,
    TRY_CAST(data_atualizacao AS TIMESTAMP) AS data_atualizacao,
    TRY_CAST(data_geracao AS TIMESTAMP) AS data_geracao,
    classificacao,
    forma_pagamento,
    beneficiario,
    statusfatura,
    moeda,
    condicoes_pagamento,
    valor_total,
    multa,
    juros,
    valor_sem_multa_juros,
    valor_pago,
    id_legado,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao_db,
    descricao_itens,
    responsavel_registro_pagamento_manual_nome,
    responsavel_registro_pagamento_manual_login,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

  FROM latest
)

SELECT *
FROM transformed
