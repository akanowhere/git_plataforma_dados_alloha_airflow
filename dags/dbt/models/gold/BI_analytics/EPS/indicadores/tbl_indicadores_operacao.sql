{{ config(
    materialized='incremental',
    unique_key=['data_referencia']
) }}

WITH data_referencia AS (
    SELECT CAST(DATE_SUB(current_date(),1) AS DATE) AS DATA_REFERENCIA
)           
, variaveis AS (
    SELECT 
        CAST(CONCAT(YEAR(DATA_REFERENCIA), '-', MONTH(DATA_REFERENCIA), '-', '01') AS DATE) AS MES_ATUAL,
        DATEADD(MONTH, -1, CAST(CONCAT(YEAR(DATA_REFERENCIA), '-', MONTH(DATA_REFERENCIA), '-', '01') AS DATE)) AS MES_ANTERIOR,
        YEAR(DATA_REFERENCIA) AS ANO_ATUAL,
		CASE WHEN DATA_REFERENCIA <= '2023-05-31' THEN 4 ELSE 2 END AS DIAS_AGING_ALTAS,
	    1440 AS TEMPO_REPARO_24HORAS,
		30 AS DIAS_REPETIDA,
		15 AS DIAS_IFI
    FROM data_referencia
)
/*###############################*/
/* ####     INDICADORES     #### */
/*###############################*/

/*TABELA FINAL EFETIVIDADE */
, tmp_final_efetividade AS (
	SELECT
		COALESCE(regional, '') AS regional,
		COALESCE(unidade, '') AS unidade,
		COALESCE(terceirizada, '') AS terceirizada,
		COALESCE(metrica1, 0) AS metrica1,
		COALESCE(metrica2, 0) AS metrica2,
		COALESCE(metrica_aux, 0) AS metrica_aux,
		'ATIVACOES_REALIZADAS' AS metrica1_nome,
		'ORDENS_IMPRODUTIVAS' AS metrica2_nome,
		'ORDENS_AGENDADAS' AS metrica_aux_nome,
		'EFETIVIDADE' AS indicador,
		(SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
	FROM (
		SELECT
			regional,
			unidade,
			terceirizada,
			SUM( CASE WHEN conclusao IN ('Realizado') THEN 1 ELSE 0 END ) AS metrica1,
		    SUM( CASE WHEN conclusao IN ('Não Realizado') THEN 1 ELSE 0 END ) AS metrica2,
			COUNT( conclusao ) AS metrica_aux
		FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
		WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'EFETIVIDADE'
		GROUP BY
			regional,
			unidade,
			Terceirizada
	) tbl_final
	UNION
	SELECT 
		'Consolidado' AS regional,
		'' AS unidade,
		'' AS terceirizada,
		0 AS metrica1,
		0 AS metrica2,
		0 AS metrica_aux,
		'ATIVACOES_REALIZADAS' AS metrica1_nome,
		'ORDENS_IMPRODUTIVAS' AS metrica2_nome,
		'ORDENS_AGENDADAS' AS metrica_aux_nome,
		'EFETIVIDADE' AS indicador,
		(SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)
/* TABELA FINAL AGING ALTAS */
, tmp_final_aging_altas AS (
    SELECT 
        COALESCE(regional, '') AS regional,
        COALESCE(unidade, '') AS unidade,
        COALESCE(Terceirizada, '') AS terceirizada,
        COALESCE(NO_PADRAO, 0) AS metrica1,
        COALESCE(ATIVACOES_REALIZADAS, 0) AS metrica2,
		0 AS metrica_aux,
        'NO_PADRAO' AS metrica1_nome,
        'ATIVACOES_REALIZADAS' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'AGING_ALTAS' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM (
        SELECT
            unidade,
            regional,
            terceirizada,
            SUM( CASE WHEN aging_categoria  = 'NO PADRÃO'  THEN 1 ELSE 0 END ) AS NO_PADRAO,
            COUNT( aging_categoria ) AS ATIVACOES_REALIZADAS
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'AGING_ALTAS'
        GROUP BY unidade, Terceirizada, regional
    ) tbl_ativacoes
    UNION
    SELECT 
		'Consolidado' AS regional,
		'' AS unidade,
		'' AS terceirizada,
		0 AS metrica1,
		0 AS metrica2,
		0 AS metrica_aux,
        'NO_PADRAO' AS metrica1_nome,
        'ATIVACOES_REALIZADAS' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'AGING_ALTAS' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)
/* TABELA BASE FINAL IFI */
, tmp_final_IFI AS (
    SELECT 
        DISTINCT 
        COALESCE(tbl_ativacoes.regional, tbl_reparos.regional) AS regional,
        COALESCE(tbl_ativacoes.unidade, tbl_reparos.unidade) AS unidade,
        COALESCE(tbl_ativacoes.terceirizada,tbl_reparos.terceirizada) AS terceirizada,
        COALESCE(tbl_reparos.infancia, 0) AS metrica1,
        COALESCE(tbl_ativacoes.ATIVACOES_REALIZADAS,0) AS metrica2,
        0 AS metrica_aux,
        'INFÂNCIA' AS metrica1_nome,
        'ATIVACOES_REALIZADAS' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'IFI' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM (
        SELECT
            terceirizada, 
            unidade,
            regional,
            COUNT( aging_categoria ) AS ATIVACOES_REALIZADAS
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'AGING_ALTAS'
        GROUP BY terceirizada, unidade, regional
    ) AS tbl_ativacoes
    FULL OUTER JOIN (
            SELECT
            terceirizada, 
            unidade,
            regional,
            COUNT( codigo_chamado ) AS infancia
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'IFI'
        GROUP BY terceirizada, unidade, regional
    ) AS tbl_reparos 
        ON tbl_ativacoes.unidade = tbl_reparos.unidade 
            AND tbl_ativacoes.terceirizada = tbl_reparos.terceirizada
            AND tbl_ativacoes.regional = tbl_reparos.regional
    UNION
    SELECT 
        'Consolidado' AS regional,
        '' AS unidade,
        '' AS terceirizada,
        0 AS metrica1,
        0 AS metrica2,
        0 AS metrica_aux,
        'INFÂNCIA' AS metrica1_nome,
        'ATIVACOES_REALIZADAS' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'IFI' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)

   /* TABELA FINAL REPAROS 24HRS */
, tmp_final_24h AS (
    SELECT 
        regional,
        unidade,
        terceirizada,
        COALESCE(tbl_reparos.reparos_24h,0) AS metrica1,
        COALESCE(tbl_reparos.reparos_total,0) AS metrica2,
        COALESCE(tbl_reparos.reparos_18h,0) AS metrica_aux,
        'REPAROS_24h' AS metrica1_nome,
        'REPAROS_TOTAIS' AS metrica2_nome,
        'REPAROS_18h' AS metrica_aux_nome,
        'REPAROS_24h' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM (
        SELECT
            regional,
            unidade,
            terceirizada,
            SUM( CASE WHEN aging_categoria IN ('Dentro de 18 hrs','Dentro de 24 hrs') THEN 1 ELSE 0 END ) AS reparos_24h,
            COUNT( aging_categoria ) AS reparos_total,
            SUM( CASE WHEN aging_categoria IN ('Dentro de 18 hrs') THEN 1 ELSE 0 END ) AS reparos_18h
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'REPAROS_24H'
        GROUP BY regional,
            unidade,
            terceirizada
    ) AS tbl_reparos
    UNION
    SELECT 
        'Consolidado' AS regional,
        '' AS unidade,
        '' AS terceirizada,
        0 AS metrica1,
        0 AS metrica2,
        0 AS metrica_aux,
        'REPAROS_24h' AS metrica1_nome,
        'REPAROS_TOTAIS' AS metrica2_nome,
        'REPAROS_18h' AS metrica_aux_nome,
        'REPAROS_24h' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)
/* TABELA BASE FINAL IRR */
,   tmp_final_IRR AS (
    SELECT 
        regional,
        unidade,
        terceirizada,
        COALESCE(tbl_irr.repetidas, 0) AS metrica1,
        COALESCE(tbl_irr.reparos, 0) AS metrica2,
        0 AS metrica_aux,
        'REPETIDAS' AS metrica1_nome,
        'REPAROS_TOTAIS' AS metrica2_nome,
         '' AS metrica_aux_nome,
        'IRR' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM (
        SELECT
            regional,
            unidade,
            terceirizada, 
            COUNT(codigo_chamado) AS reparos,
            SUM(CASE WHEN tecnico_anterior <> ' - ' THEN 1 ELSE 0 END) AS repetidas
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia)  AND indicador = 'IRR'
        GROUP BY 
            terceirizada,
            unidade,
            regional
    ) AS tbl_irr
    UNION
    SELECT 
        'Consolidado' AS regional,
        '' AS unidade,
        '' AS terceirizada,
        0 AS metrica1,
        0 AS metrica2,
        0 AS metrica_aux,
        'REPETIDAS' AS metrica1_nome,
        'REPAROS_TOTAIS' AS metrica2_nome,
         '' AS metrica_aux_nome,
        'IRR' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)

/* TABELA BASE FINAL IRT */
, tmp_final_IRT AS (
            SELECT 
        DISTINCT
        tbl_irt.regional,
        COALESCE(tbl_irt.unidade, tbl_base.unidade) AS unidade,
        COALESCE(tbl_irt.terceirizada, '') AS terceirizada,
        COALESCE(tbl_irt.reparos_projetado, 0) AS metrica1,
        COALESCE(tbl_base.media,0) AS metrica2,
        0 AS metrica_aux,
        'REPAROS_PROJETADOS' AS metrica1_nome,
        'BASE_MÉDIA' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'IRT' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM (
            SELECT 
            regional,
            unidade,
            terceirizada,
            CAST(ROUND((COUNT(DISTINCT codigo_chamado) + (
                    (COUNT(DISTINCT codigo_chamado)*1.0 / DAY((SELECT DATA_REFERENCIA FROM data_referencia))) * (DAY(LAST_DAY((SELECT DATA_REFERENCIA FROM data_referencia))) - DAY( (SELECT DATA_REFERENCIA FROM data_referencia))))
            ),0) AS INT) AS reparos_projetado
        FROM {{ get_catalogo('gold') }}.operacao.tbl_analiticos_operacao
        WHERE data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) AND indicador = 'IRT'
        GROUP BY unidade, regional, terceirizada
    ) tbl_irt 
    FULL OUTER JOIN (
        SELECT 
            unidade,
            ROUND((BASE_D1 + BASE_M1)/2,0) AS media
        FROM(
            SELECT 
                unidade, 
                SUM(CASE WHEN data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) THEN qtd_ativos ELSE 0 END) AS BASE_D1,
                SUM(CASE WHEN data_referencia = DATEADD(DAY, -DAY((SELECT DATA_REFERENCIA FROM data_referencia)), (SELECT DATA_REFERENCIA FROM data_referencia)) THEN qtd_ativos ELSE 0 END) AS BASE_M1
            FROM {{ get_catalogo('gold') }}.base.fato_base_ativos ba
            LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u
                ON ba.unidade = u.sigla AND u.fonte = 'AIR' and u.excluido = false
            WHERE 
                ba.fonte = 'AIR' 
                AND data_referencia IN ((SELECT DATA_REFERENCIA FROM data_referencia), DATEADD(DAY, -DAY((SELECT DATA_REFERENCIA FROM data_referencia)), (SELECT DATA_REFERENCIA FROM data_referencia)))
                AND u.marca IS NOT NULL
            GROUP BY unidade
        )tbl_media
    ) tbl_base ON(tbl_irt.unidade = tbl_base.unidade)
    WHERE terceirizada IS NOT NULL
    UNION
    SELECT 
        'Consolidado' AS regional,
        '' AS unidade,
        '' AS terceirizada,
        0 AS metrica1,
        0 AS metrica2,
        0 AS metrica_aux,
        'REPAROS_PROJETADOS' AS metrica1_nome,
        'BASE_MÉDIA' AS metrica2_nome,
        '' AS metrica_aux_nome,
        'IRT' AS indicador,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
)

SELECT *
	FROM(
		(SELECT * FROM tmp_final_24h)
		UNION
		(SELECT * FROM tmp_final_aging_altas)
		UNION
		(SELECT * FROM tmp_final_efetividade)
		UNION
		(SELECT * FROM tmp_final_IRT)
		UNION
		(SELECT * FROM tmp_final_IRR)
		UNION
		(SELECT * FROM tmp_final_IFI)
	)tbl_indicadores