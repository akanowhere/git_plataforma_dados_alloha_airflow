WITH source AS (
  SELECT *
  FROM {{ source("air_comercial", "tbl_contrato_campanha") }}
),

transformed AS (
  SELECT
    id,
    id_contrato,
    id_campanha,
    TRY_CAST(recorrencia_vigencia AS DATE) AS recorrencia_vigencia,
    cancelada,
    id_venda,
    recorrencia_percentual_desconto,
    recorrencia_meses_desconto,
    instalacao_item_codigo,
    instalacao_item_nome,
    instalacao_empresa_nome,
    instalacao_empresa_cnpj,
    instalacao_quantidade_parcela,
    instalacao_valor,
    instalacao_desconto_percentual,
    recorrencia_desconto_aplicado,
    recorrencia_desconto_data_venda

  FROM source
)

SELECT *
FROM transformed
