WITH source AS (
  SELECT *
  FROM {{ source("air_comercial", "tbl_cliente_fisico") }}
),

transformed AS (
  SELECT
    id_cliente,
    cpf,
    TRY_CAST(data_nascimento AS DATE) AS data_nascimento,
    rg_numero,
    rg_orgao,
    estado_civil,
    nome_pai,
    nome_mae

  FROM source
)

SELECT *
FROM transformed
