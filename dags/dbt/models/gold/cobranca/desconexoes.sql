SELECT
    C.id_contrato,
    C.CODIGO_CONTRATO_AIR,
    C.CODIGO_CONTRATO_IXC,
    C.data_cancelamento,
    C.data_ativacao,
    C.motivo_cancelamento,
    C.cidade,
    C.estado,
    IFNULL(F.canal, FO.canal) AS canal,
    C.marca,
    C.unidade,
    C.produto,
    C.pacote_tv,
    C.dia_fechamento,
    C.dia_vencimento,
    C.aging,
    C.segmento,
    IFNULL(F.equipe, FO.equipe) AS equipe,
    C.tipo_cancelamento,
    C.sub_motivo_cancelamento,
    C.cancelamento_invol,
    C.pacote_nome,
    C.valor_final,
    C.cluster,
    C.fonte,
    C.legado_sistema,
    C.legado_id,
    IFNULL(F.canal_tratado, FO.canal_tratado) AS canal_tratado,
    IFNULL(F.tipo_canal, FO.tipo_canal) AS tipo_canal,
    C.aging_meses_cat,
    C.flg_fidelizado,
    CO.id_cliente AS codigo_cliente_air,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_atualizacao
FROM {{ ref('fato_cancelamento') }} C
LEFT JOIN gold.venda.fato_venda F
    ON (F.id_contrato_air = C.CODIGO_CONTRATO_AIR AND F.id_contrato_air IS NOT NULL)
LEFT JOIN {{ ref('fato_venda') }} FO
    ON (FO.id_contrato_air IS NULL AND FO.fonte = C.fonte AND FO.id_contrato_ixc = C.CODIGO_CONTRATO_IXC)
LEFT JOIN {{ ref('dim_contrato') }} CO
    ON (C.CODIGO_CONTRATO_AIR = CO.id_contrato_air)
INNER JOIN {{ get_catalogo('silver') }}.stage_seeds_data.dim_marca m
    ON (C.marca = m.marca AND m.ativo = 1)
WHERE C.data_cancelamento >= '2022-01-01'
    AND (
        (C.fonte = 'AIR' AND C.data_cancelamento >= m.data_migracao)
        OR
        (C.fonte <> 'AIR' AND C.data_cancelamento < m.data_migracao)
    )
