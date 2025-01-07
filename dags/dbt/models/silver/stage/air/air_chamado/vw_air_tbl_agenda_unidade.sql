WITH source AS (
  SELECT
    id_agenda,
    unidades,
    _metadata.file_modification_time AS data_extracao

  FROM {{ source("air_chamado", "tbl_agenda_unidade") }}
)

SELECT *
FROM source
