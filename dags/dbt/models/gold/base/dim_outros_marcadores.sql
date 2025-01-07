WITH vw_air_tbl_outros_marcadores AS (
    SELECT *
    FROM {{ ref('vw_air_tbl_outros_marcadores') }}
),

transformed AS (
   SELECT
        id
        ,id_cliente
        ,marcador
        ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

    FROM  vw_air_tbl_outros_marcadores
)

SELECT * FROM transformed;
