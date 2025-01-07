WITH source AS (
  SELECT *
  FROM {{ source("air_comercial", "tbl_contrato_migracao") }}
),

transformed AS (
  SELECT
    id,
    id_venda,
    id_contrato,
    antigo_pacote_codigo,
    antigo_pacote_nome,
    antigo_valor,
    antigo_versao,
    novo_pacote_codigo,
    novo_pacote_nome,
    novo_valor,
    novo_versao,
    reprovada

  FROM source
)

SELECT *
FROM transformed
