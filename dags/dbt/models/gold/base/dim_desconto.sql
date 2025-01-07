WITH

vw_air_tbl_desconto AS (
    SELECT *
    FROM {{ ref('vw_air_tbl_desconto') }}
),

transformed AS (
   SELECT
        C.id
        ,DATE_FORMAT(C.data_criacao,'yyyy-MM-dd HH:mm:ss') AS data_criacao
        ,DATE_FORMAT(C.data_alteracao,'yyyy-MM-dd HH:mm:ss') AS data_alteracao
        ,usuario_criacao
        ,usuario_alteracao
        ,excluido
        ,id_contrato
        ,desconto
        ,data_validade
        ,dia_aplicar
        ,item_codigo
        ,item_nome
        ,tipo
        ,categoria
        ,observacao
        ,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

    FROM  vw_air_tbl_desconto AS C 
)

SELECT * FROM transformed;
