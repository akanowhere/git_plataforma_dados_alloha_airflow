WITH fato_ativacao AS (
    SELECT id_contrato,
        data_ativacao,
        cidade,
        regiao,
        estado,
        canal,
        marca,
        data_extracao,
        unidade,
        regional,
        produto,
        pacote_tv,
        tipo_tv,
        segmento,
        cluster,
        fonte,
        legado_id,
        legado_sistema,
        equipe,
        canal_tratado,
        tipo_canal,
        CAST(
            CASE
                WHEN fonte = 'AIR' THEN id_contrato
                ELSE NULL
            END AS BIGINT
        ) AS id_contrato_air,
        CAST(
            CASE
                WHEN fonte <> 'AIR' THEN id_contrato
                WHEN legado_sistema = 'ixc_click' THEN legado_id
                ELSE NULL
            END AS BIGINT
        ) AS id_contrato_ixc,
        macro_regional,
        nome_regional,
        data_criacao_contrato,
        tecnico,
        terceirizada

    FROM (
            SELECT id_contrato,
                data_ativacao,
                {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.cidade,
                regiao,
                estado,
                canal,
                CASE
                    WHEN legado_sistema = 'ixc_click' THEN 'CLICK'
                    ELSE UPPER({{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.marca)
                END AS marca,
                data_extracao,
                unidade_atendimento AS unidade,
                COALESCE(
                    a.sigla_regional,
                    {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.regional
                ) AS regional,
                produto,
                pacote_tv,
                tipo_tv,
                segmento,
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
                equipe,
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
                a.macro_regional AS macro_regional,
                a.regional1 AS nome_regional,
                {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato.data_cadastro_sistema as data_criacao_contrato,
                usr.nome as tecnico,
                vnd.terceirizada

            FROM {{ get_catalogo('silver') }}.stage_contrato.vw_situacao_contrato

            LEFT JOIN ( SELECT
                            codigo_contrato,
                            id_tecnico
                        FROM {{ ref('vw_air_tbl_chd_chamado') }}) chd on id_contrato = chd.codigo_contrato

            LEFT JOIN ( SELECT
                            id,
                            usuario,
                            terceirizada
                        FROM {{ ref('vw_air_tbl_vendedor') }}) vnd on vnd.id = chd.id_tecnico

            LEFT JOIN ( SELECT
                            nome,
                            codigo
                        FROM {{ ref('vw_air_tbl_usuario') }}) usr on usr.codigo = vnd.usuario

                LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais a
                ON (REPLACE(REPLACE(UPPER({{ translate_column('vw_situacao_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(a.cidade_sem_acento)
                    OR REPLACE(REPLACE(UPPER({{ translate_column('vw_situacao_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') = UPPER(a.cidade))
                AND UPPER(vw_situacao_contrato.estado) = UPPER(a.uf)
                LEFT JOIN {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_consultor aa on aa.contract_air_id = id_contrato
            WHERE CAST(data_ativacao AS DATE) <= date_sub(current_date(), 1)
                AND (
                    legado_sistema NOT IN (
                        'ixc_click',
                        'integrator_univox',
                        'ixc_univox',
                        'protheus_ligue',
                        'ng_vip',
                        'mk_niu',
                        'ng_niu',
                        'ng_pamnet',
                        'mk_pamnet',
                        'adapter',
                        'URBE'
                    )
                    OR legado_sistema IS NULL
                )
                AND (
                    legado_sistema <> 'ixc_click'
                    OR legado_sistema = 'ixc_click'
                    AND CAST(data_ativacao AS DATE) >= CAST('2023-03-13' AS DATE)
                    OR (legado_sistema IS NULL)
                )
                AND (
                    legado_sistema <> 'ixc_univox'
                    OR legado_sistema = 'ixc_univox'
                    AND CAST(data_ativacao AS DATE) >= CAST('2023-05-08' AS DATE)
                    OR (legado_sistema IS NULL)
                )
        ) A

    UNION ALL

    SELECT
        *,
        NULL AS data_criacao_contrato,
        NULL AS tecnico,
        NULL AS terceirizada
    FROM {{ get_catalogo('silver') }}.stage_legado.vw_fato_ativacao
)
SELECT DISTINCT id_contrato,
    id_contrato_air AS CODIGO_CONTRATO_AIR,
    id_contrato_ixc AS CODIGO_CONTRATO_IXC,
    data_ativacao,
    {{ translate_column('cidade') }} AS cidade,
    estado,
    marca,
    unidade,
    regional,
    {{ translate_column('produto') }} AS produto,
    UPPER(tipo_tv) AS pacote_tv,
    segmento,
    cluster,
    fonte,
    legado_sistema,
    legado_id,
    macro_regional,
    nome_regional,
    data_criacao_contrato,
    tecnico,
    terceirizada
FROM fato_ativacao
