WITH source AS (
  SELECT
    id_tecnico,
    unidades,
    _metadata.file_modification_time AS data_extracao

  FROM {{ source('air_chamado', 'tbl_chd_tecnico_unidade') }}
)

SELECT *
FROM source
