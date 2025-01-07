WITH source AS (
  SELECT *
  FROM {{ source('mailing', 'fatura') }}
),

transformed AS (
  SELECT
    codigo_fatura,
    contrato,
    status_fatura,
    classificacao_fatura,
    TRY_CAST(data_criacao_fatura AS TIMESTAMP) AS data_criacao_fatura,
    TRY_CAST(data_vencimento_fatura AS TIMESTAMP) AS data_vencimento_fatura,
    TRY_CAST(data_pagamento_fatura AS TIMESTAMP) AS data_pagamento_fatura,
    TRY_CAST(valor_fatura_emitida AS DOUBLE) AS valor_fatura_emitida,
    TRY_CAST(valor_pago AS DOUBLE) AS valor_pago,
    carteira_cobranca,
    marca,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao

  FROM source

  WHERE codigo_fatura IS NOT NULL
)

SELECT *
FROM transformed
