WITH source AS (
  SELECT
    id,
    hierarquia_fechamento,
    ativo,
    _metadata.file_modification_time AS data_extracao

  FROM {{ source('air_chamado', 'tbl_os_hierarquia_fechamento') }}
)

SELECT *
FROM source
