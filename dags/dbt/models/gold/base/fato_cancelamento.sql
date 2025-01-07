WITH fato_cancelamento AS (
    SELECT
        A.id_contrato,
        A.data_cancelamento,
        A.data_ativacao,
        A.motivo_cancelamento,
        A.cidade,
        A.regiao,
        A.estado,
        A.canal,
        CASE
            WHEN A.legado_sistema = 'ixc_click' THEN 'CLICK'
            ELSE UPPER(A.marca)
        END AS marca,
        A.data_extracao,
        A.unidade,
        A.regional,
        A.produto,
        A.pacote_tv,
        A.tipo_tv,
        A.dia_fechamento,
        A.dia_vencimento,
        A.aging,
        A.segmento,
        A.equipe,
        A.tipo_cancelamento,
        A.sub_motivo_cancelamento,
        A.cancelamento_invol,
        A.pacote_nome,
        A.valor_final,
        A.cluster,
        A.fonte,
        A.legado_id,
        A.legado_sistema,
        A.canal_tratado,
        A.tipo_canal,
        CAST(
            CASE
                WHEN A.fonte = 'AIR' THEN A.id_contrato
                ELSE NULL
            END AS BIGINT
        ) AS id_contrato_air,
        CAST(
            CASE
                WHEN A.fonte <> 'AIR' THEN A.id_contrato
                WHEN A.legado_sistema = 'ixc_click' THEN A.legado_id
                ELSE NULL
            END AS BIGINT
        ) AS id_contrato_ixc,
        A.flg_fidelizado,
        A.macro_regional,
        A.nome_regional
    FROM (
            SELECT id_contrato,
                data_cancelamento,
                data_ativacao,
                motivo_cancelamento,
                REPLACE(REPLACE(UPPER(
                        {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.cidade
                    ), CHAR(13), ''), CHAR(10), '') AS cidade,
                UPPER(regiao) AS regiao,
                UPPER(estado) AS estado,
                UPPER(canal) AS canal,
                UPPER({{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.marca) AS marca,
                data_extracao,
                unidade_atendimento AS unidade,
                UPPER(COALESCE(a.sigla_regional, sigla_regional)) AS regional,
                produto,
                pacote_tv,
                tipo_tv,
                dia_fechamento,
                dia_vencimento,
                DATEDIFF(DAY, data_ativacao, data_cancelamento) AS aging,
                segmento,
                equipe,
                CASE
                    WHEN motivo_cancelamento = 'CM_ERRO_MIGRACAO'
                    AND data_ativacao IS NOT NULL
                    AND data_cancelamento >= '2023-05-01' THEN 'INVOLUNTARIO'
                    ELSE tipo_cancelamento
                END AS tipo_cancelamento,
                sub_motivo_cancelamento,
                UPPER(cancelamento_invol) AS cancelamento_invol,
                plano AS pacote_nome,
                valor_contrato AS valor_final,
                REPLACE(
                    COALESCE(
                        a.subregional,
                        {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.cluster
                    ),
                    'CLUSTER_',
                    ''
                ) AS cluster,
                fonte,
                legado_id,
                legado_sistema,
                CASE
                    WHEN canal = 'TLV RECEPTIVO' THEN 'RECEPTIVO'
                    WHEN aa.contract_air_id IS NOT NULL THEN 'GIGA EMBAIXADOR'
                    WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 'GIGA EMBAIXADOR'
                    ELSE canal
                END AS canal_tratado,
                CASE
                    WHEN aa.contract_air_id IS NOT NULL THEN 'TERCEIRIZADO'
                    WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 'TERCEIRIZADO'
                    ELSE tipo_canal
                END AS tipo_canal,
                flg_fidelizado,
                a.macro_regional AS macro_regional,
                a.regional1 AS nome_regional
            FROM {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato
                LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais a
                ON (REPLACE(REPLACE(UPPER({{ translate_column('vw_situacao_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(a.cidade_sem_acento)
                    OR REPLACE(REPLACE(UPPER({{ translate_column('vw_situacao_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(a.cidade))
                AND UPPER(vw_situacao_contrato.estado) = UPPER(a.uf)
                LEFT JOIN {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_consultor aa ON aa.contract_air_id = id_contrato
                WHERE CAST(data_cancelamento AS DATE) <= date_sub(current_date(), 1)
                AND (
                    motivo_cancelamento <> 'BASE NEWFIBRA'
                    OR motivo_cancelamento IS NULL
                )
                AND (
                    legado_sistema <> 'ixc_click'
                    OR legado_sistema = 'ixc_click'
                    AND CAST(data_cancelamento AS DATE) >= CAST('2023-03-13' AS DATE)
                    OR (legado_sistema IS NULL)
                )
                AND (
                    legado_sistema <> 'ixc_univox'
                    OR legado_sistema = 'ixc_univox'
                    AND CAST(data_cancelamento AS DATE) >= CAST('2023-05-08' AS DATE)
                    OR (legado_sistema IS NULL)
                )
        ) A
    WHERE (UPPER(A.motivo_cancelamento) NOT IN (
            'LEGADO NIU',
            'LEGADO PAMNET',
            'LEGADO VIP',
            'LEGADO LIGUE',
            'LEGADO MOB'
        ) OR A.motivo_cancelamento IS NULL)
    AND A.id_contrato not in (select CODIGO_CONTRATO_AIR from {{ get_catalogo('bronze') }}.legado.expurgo_fato_cancelamento)

    UNION ALL

    SELECT *
        FROM {{ get_catalogo('silver') }}.stage_legado.vw_fato_cancelamento
)

SELECT DISTINCT *,
    CASE
        WHEN aging < 90 THEN 'Abaixo 3 meses'
        WHEN aging >= 90 AND aging < 180 THEN 'Entre 3 e 6 meses'
        WHEN aging >= 180 AND aging < 270 THEN 'Entre 6 e 9 meses'
        WHEN aging >= 270 AND aging < 365 THEN 'Entre 9 e 12 meses'
        WHEN aging >= 365 AND aging < 545 THEN 'Entre 13 e 18 meses'
        WHEN aging >= 545 AND aging < 720 THEN 'Entre 19 e 24 meses'
        WHEN aging >= 720 THEN 'Acima de 24 meses'
        ELSE ''
    END AS aging_meses_cat
FROM (
    SELECT DISTINCT
        id_contrato,
        id_contrato_air AS CODIGO_CONTRATO_AIR,
        id_contrato_ixc AS CODIGO_CONTRATO_IXC,
        data_cancelamento,
        data_ativacao,
        UPPER({{ translate_column('motivo_cancelamento') }}) AS motivo_cancelamento,
        UPPER({{ translate_column('cidade') }}) AS cidade,
        UPPER(estado) AS estado,
        UPPER({{ translate_column('canal') }}) AS canal,
        UPPER(marca) AS marca,
        UPPER(unidade) AS unidade,
        UPPER(regional) AS regional,
        UPPER(produto) AS produto,
        UPPER(tipo_tv) AS pacote_tv,
        CAST(dia_fechamento AS INTEGER) AS dia_fechamento,
        CAST(dia_vencimento AS INTEGER) AS dia_vencimento,
        CAST(aging AS INTEGER) AS aging,
        UPPER(segmento) AS segmento,
        UPPER(equipe) AS equipe,
        UPPER({{ translate_column('tipo_cancelamento') }}) AS tipo_cancelamento,
        {{ translate_column('sub_motivo_cancelamento') }} AS sub_motivo_cancelamento,
        UPPER({{ translate_column('cancelamento_invol') }}) AS cancelamento_invol,
        pacote_nome,
        valor_final,
        cluster,
        fonte,
        legado_sistema,
        legado_id,
        canal_tratado,
        tipo_canal,
        flg_fidelizado,
        macro_regional,
        nome_regional,
        data_extracao
    FROM fato_cancelamento
) A
