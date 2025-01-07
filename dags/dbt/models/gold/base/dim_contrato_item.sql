WITH

vw_air_tbl_contrato_item AS (
    SELECT *
    FROM {{ ref('vw_air_tbl_contrato_item') }}
),

vw_air_tbl_contrato_produto AS (
    SELECT *
    FROM {{ ref('vw_air_tbl_contrato_produto') }}
),

joined AS (
    SELECT
        ctr_item.id AS id_contrato_item,
        ctr_produto.id AS id_contrato_produto,
        ctr_produto.id_contrato,
        ctr_item.usuario_alteracao,
        ctr_item.excluido,
        ctr_item.valor_unitario,
        ctr_item.valor_base,
        ctr_item.quantidade,
        ctr_item.valor_final,
        ctr_item.item_codigo,
        ctr_item.item_nome,
        ctr_produto.data_alteracao AS data_alteracao_produto,
        ctr_produto.usuario_alteracao AS usuario_alteracao_produto,
        ctr_produto.excluido AS excluido_produto,
        ctr_produto.versao_contrato AS versao_contrato_produto,
        ctr_produto.item_codigo AS item_codigo_produto,
        ctr_produto.item_nome AS item_nome_produto,
        ctr_produto.item_grupo AS item_grupo_produto,
        ctr_produto.adicional AS adicional_produto,
        DATE_FORMAT(ctr_item.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
        CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

    FROM vw_air_tbl_contrato_item ctr_item
    LEFT JOIN vw_air_tbl_contrato_produto ctr_produto ON ctr_produto.id = ctr_item.id_contrato_produto
)

SELECT
    *
FROM
    joined
