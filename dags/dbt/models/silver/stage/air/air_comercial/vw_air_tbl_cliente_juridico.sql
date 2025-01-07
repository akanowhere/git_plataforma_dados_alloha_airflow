WITH source AS (
  SELECT *
  FROM {{ source("air_comercial", "tbl_cliente_juridico") }}
),

transformed AS (
  SELECT
    id_cliente,
    cnpj,
    inscricao_estadual,
    inscricao_municipal,
    nome_fantasia,
    atividade_principal

  FROM source
)

SELECT *
FROM transformed
