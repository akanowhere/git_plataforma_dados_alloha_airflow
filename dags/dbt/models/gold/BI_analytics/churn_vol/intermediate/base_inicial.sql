SELECT 
    DISTINCT
    cadastro.id_contrato_air contrato_air,
    cadastro.status status_contrato,
    cadastro.id_cliente id_cliente_air,
    cadastro.data_criacao data_abertura_cadastro,
    cadastro.data_primeira_ativacao ativacao_contrato,
    cadastro.pacote_nome,
    date_format(cadastro.data_primeira_ativacao, 'yyyyMM') as ano_mes_ativacao,
    desconexoes.data_cancelamento,
    date_format(desconexoes.data_cancelamento, 'yyyyMM') as ano_mes_cancelamento,
    desconexoes.tipo_cancelamento,
    uni.marca,
    uni.regional,
    uni.`cluster`,
    tbl_guerra.classificacao_guerra,
    DE.cep,
    DE.estado,
    DE.cidade,
    cadastro.dia_vencimento,
    cadastro.fechamento_vencimento,
    CASE WHEN mob.contrato_air IS NOT NULL THEN 1 ELSE 0 END AS flag_legado_mob,
    CASE
        WHEN uni.regional = 'R4' THEN 'SUDESTE 1'
        WHEN uni.regional = 'R9' THEN 'SUDESTE 2'
        WHEN UPPER(uni.marca) IN ('NIU','PAMNET','VIP','LIGUE') THEN 'SUDESTE 1'
        WHEN UPPER(uni.marca) IN ('SUMICITY','GIGA+ FIBRA','UNIVOX','CLICK') THEN 'SUDESTE 2'
        WHEN UPPER(uni.marca) IN ('MOB') THEN 'NORDESTE'
        ELSE 'Não classificado'
    END AS Regiao
FROM gold.base.dim_contrato cadastro
LEFT JOIN gold.base.dim_cliente cliente
    ON cadastro.id_cliente = cliente.id_cliente
LEFT JOIN gold.base.fato_cancelamento desconexoes
    ON cadastro.id_contrato_air = desconexoes.CODIGO_CONTRATO_AIR AND tipo_cancelamento in ('INVOLUNTARIO','VOLUNTARIO')
LEFT JOIN silver.stage_legado.vw_mailing_base_legado_nao_cobravel_mob mob
    ON cadastro.id_contrato_air = mob.contrato_air
LEFT JOIN gold.base.dim_unidade uni
    ON cadastro.unidade_atendimento = uni.sigla AND uni.fonte = 'AIR' and uni.excluido = false
LEFT JOIN (
        SELECT A.id_contrato, MAX(A.id_endereco) AS id_endereco
        FROM gold.base.dim_contrato_entrega A 
        GROUP BY A.id_contrato
    ) AS CE 
        ON CE.id_contrato = cadastro.id_contrato_air
    LEFT JOIN gold.base.dim_endereco DE 
        ON CE.id_endereco = DE.id_endereco
LEFT JOIN gold_dev.churn_voluntario.tbl_classificacao_guerra tbl_guerra
    ON tbl_guerra.cidade = DE.cidade AND tbl_guerra.territorio = REPLACE(uni.regional, 'R','T')
WHERE CAST(data_primeira_ativacao AS DATE) IS NOT NULL AND cliente.segmento <> 'B2B'
AND cadastro.pacote_nome <> 'CONTRATO CANCELADO MIG'
AND cadastro.pacote_nome <> 'MIG CONTRATO CANCELADO'
AND NOT (cadastro.data_cancelamento is null AND cadastro.status LIKE '%CANCEL%')