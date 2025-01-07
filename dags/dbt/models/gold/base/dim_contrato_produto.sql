WITH

vw_air_tbl_contrato_produto AS (
    SELECT *
    FROM {{ ref('vw_air_tbl_contrato_produto') }}
),

transformed AS (
    SELECT
        id AS id_contrato_produto,
        data_criacao,
        usuario_criacao,
        data_alteracao,
        usuario_alteracao,
        excluido,
        id_contrato,
        versao_contrato,
        item_codigo,
        item_nome,
        item_grupo,
        data_instalacao,
        data_cancelamento,
        adicional,
        CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

    FROM vw_air_tbl_contrato_produto
)

SELECT
    *
FROM
    transformed
