{{ config(
    materialized='table'
) }}

/* TABELA DE FILAS DE REPARO */
WITH FILAS_REPARO AS (
    SELECT 
        * 
    FROM (VALUES
        ('REPARO TERCEIRIZADA'),
        ('REPARO EMPRESARIAL'),
        ('REPARO - PME'),
        ('REPARO')
    ) AS t(fila)
) 
/* TABELA DE MOTIVOS DE EXPURGO IRT */
, MOTIVOS_IRT AS (
	SELECT 
        * 
    FROM (VALUES
        ('MTV_INCOMPATIVEL_TV'),
        ('CANCEL_BOOT_ONU'),
        ('Tipo de OS Incorreta')
    ) AS t(motivo)
)

/* TABELA DE MOTIVOS DE EXPURGO REAGENDADA */
, MOTIVOS_REAGENDADA AS(
    SELECT
        *
    FROM (VALUES
        ('Sem Energia Elétrica Na Residência'),
        ('RMA DE AGENDAMENTO CLIENTE'),
        ('Residência Em Construção / Reforma'),
        ('Reagendamento Solicitado Pelo Cliente'),
        ('Entrada NÃO Autorizada'),
        ('DESISTENCIA DA ASSINATURA OU SERVIÇO'),
        ('Desistência da Assinatura / Serviço'),
        ('Cliente NÃO solicitou serviço'),
        ('CLIENTE NÃO ESTARÁ DISPONIVEL'),
        ('CLIENTE AUSENTE'),
        ('Tipo de OS Incorreta')
    ) AS t(motivo)
)
/* TABELA DE EXPURGO URBE */
,tmp_expurgo_urbe AS (
    SELECT id_contrato_air as codigo_contrato
    FROM {{ get_catalogo('gold') }}.base.dim_contrato
    WHERE pacote_nome LIKE ANY ('50Mega URBE','500Mega SCM','GIGA','BANDA LARGA 500 MBPS PREFEITURA MARINGA','BANDA LARGA 1 GBPS PREFEITURA MARINGA') 
)
,tmp_chamados AS(
    SELECT 
        fo.*, 
        DE.bairro,
        u.nome AS cidade,
        u.regional,
        u.marca,
        u.cluster,
        COALESCE(UPPER(de_para.terceirizada_air), 'GIGA+') AS terceirizada_corrigida
    FROM {{ get_catalogo('gold') }}.chamados.dim_ordem fo
    LEFT JOIN {{ get_catalogo('gold') }}.operacao.tbl_depara_terceirizadas_operacao de_para
        ON fo.posicao_sap = de_para.de
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u
        ON fo.unidade = u.sigla AND u.fonte = 'AIR' and u.excluido = false
    LEFT JOIN (
        SELECT A.id_contrato, MAX(A.id_endereco) AS id_endereco
        FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega A 
        GROUP BY A.id_contrato
    ) AS CE 
        ON CE.id_contrato = fo.codigo_contrato
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco DE 
        ON CE.id_endereco = DE.id_endereco
    WHERE 
        NOT codigo_contrato IN (SELECT codigo_contrato FROM tmp_expurgo_urbe)
        AND u.marca IS NOT NULL   
)
, tmp_reparos AS (
    SELECT * 
    FROM tmp_chamados
    WHERE 
    fila IN (SELECT fila FROM FILAS_REPARO)
)
, tmp_os_reparos_realizadas AS (
    SELECT
        chamado,
        codigo_os
    FROM (
        SELECT 
            chamado,
            codigo_os,
            data_evento,
            terceirizada_corrigida,
            ROW_NUMBER() OVER(PARTITION BY chamado ORDER BY codigo_os DESC) AS os_valida
        FROM tmp_reparos
        WHERE conclusao = 'Realizado'
    )
    WHERE os_valida = 1
)
/*###############################*/
/* ####     ANALÍTICOS      #### */
/*###############################*/

/* ####    Mudanças    #### */
, tmp_mudancas AS (
    SELECT *, 
        CASE 
            WHEN fila = 'UPGRADE NÃO LÓGICO' THEN 'UPGRADE'
            WHEN fila = 'MUDANÇA DE ENDEREÇO' THEN 'MUDANÇA_ENDEREÇO'
            WHEN fila = 'MUDANÇA DE CÔMODO' THEN 'MUDANÇA_CÔMODO'
        END indicador,
        ROW_NUMBER() OVER(PARTITION BY chamado ORDER BY codigo_os DESC) AS os_valida
    FROM tmp_chamados
    WHERE fila IN ('UPGRADE NÃO LÓGICO','MUDANÇA DE ENDEREÇO','MUDANÇA DE CÔMODO')
    AND conclusao = 'Realizado'
)
/* ####    Mudanças    #### */
, tmp_analitico_mudanca AS (
    SELECT 
        DISTINCT
        posicao_sap,
        terceirizada_corrigida AS terceirizada,
        bairro,
        marca,
        cidade,
        regional,
        cluster,
        unidade,
        codigo_contrato,
        chamado AS codigo_chamado,
        codigo_os,
        numero_tarefa,
        fila,
        tecnico,
        id_tecnico,
        usuario_conclusao,
        motivo_conclusao,
        conclusao,
        sistema_origem,
        0 AS os_anterior,
        '' AS motivo_conclusao_anterior,
        '1900-01-01' AS data_conclusao_anterior,
        0 AS id_tecnico_anterior,
        '' AS tecnico_anterior,
        turno,
        data_abertura_chamado,
        data_abertura_os,
        inicio_servico AS data_inicio_servico,
        '1900-01-01' AS data_ativacao,
        '1900-01-01' AS data_criacao_contrato,
        data_evento,
        data_conclusao,
        descricao,
        0 AS protocolo,
        DATEDIFF(DAY, CAST(data_abertura_chamado AS DATE), CAST(data_conclusao AS DATE)) AS aging,
        CASE
          WHEN DATEDIFF(DAY, CAST(data_abertura_chamado AS DATE), CAST(data_conclusao AS DATE)) <= 2 THEN 'Até 2d'
          WHEN DATEDIFF(DAY, CAST(data_abertura_chamado AS DATE), CAST(data_conclusao AS DATE)) <= 4 THEN '3d a 4d'
          WHEN DATEDIFF(DAY, CAST(data_abertura_chamado AS DATE), CAST(data_conclusao AS DATE)) <= 7 THEN '5d a 7d'
          WHEN DATEDIFF(DAY, CAST(data_abertura_chamado AS DATE), CAST(data_conclusao AS DATE)) > 7 THEN 'Mais 7d'
          ELSE NULL
        END AS aging_categoria,
        indicador,
        trunc(data_evento, 'MM') mes_referencia
    FROM tmp_mudancas
    WHERE os_valida = 1
)

/* ####  Efetividade   #### */
	/* 
        Ativações pra questão de efetividade é considerado apenas ordens de serviço realizadas,
        entretando pode haver contratos que foram ativos mas o lançamento foi feito errado na ordem.
        Esses erros sistêmicos não são considerados denntro do indicador.
     */

/* ###     TABELA OS DE ATIVAÇÕES     #### */
, tmp_ativacoes_efetividade_analitico AS (
    SELECT 
        *
    FROM tmp_chamados
    WHERE 
        conclusao IN ('Realizado', 'Não Realizado')
        AND fila LIKE ANY ('ATIVA%','FTTA Novo Fluxo')
)

    /* ANALÍTICO EFETIVIDADE*/
,   tmp_analitico_efetividade AS (
		SELECT 
			DISTINCT
            posicao_sap,
			tbl_ativacoes.terceirizada_corrigida AS terceirizada,
			tbl_ativacoes.bairro,
			tbl_ativacoes.marca,
			tbl_ativacoes.cidade,
			tbl_ativacoes.regional,
			tbl_ativacoes.cluster,
			tbl_ativacoes.unidade,
			tbl_ativacoes.codigo_contrato,
			tbl_ativacoes.chamado AS codigo_chamado,
			tbl_ativacoes.codigo_os,
			tbl_ativacoes.numero_tarefa,
			tbl_ativacoes.fila,
			tbl_ativacoes.tecnico,
			tbl_ativacoes.id_tecnico,
			tbl_ativacoes.usuario_conclusao,
			tbl_ativacoes.motivo_conclusao,
			tbl_ativacoes.conclusao,
			tbl_ativacoes.sistema_origem,
            0 AS os_anterior,
            '' AS motivo_conclusao_anterior,
            '1900-01-01' AS data_conclusao_anterior,
            0 AS id_tecnico_anterior,
            '' AS tecnico_anterior,
			tbl_ativacoes.turno,
			tbl_ativacoes.data_abertura_chamado,
			tbl_ativacoes.data_abertura_os,
			tbl_ativacoes.inicio_servico AS data_inicio_servico,
			'1900-01-01' AS data_ativacao,
			'1900-01-01' AS data_criacao_contrato,
			tbl_ativacoes.data_evento,
			tbl_ativacoes.data_conclusao,
            tbl_ativacoes.descricao,
			0 AS protocolo,
			0 AS aging,
			'Sem categoria' AS aging_categoria,
			'EFETIVIDADE' AS indicador,
            trunc(data_evento, 'MM') mes_referencia
		FROM tmp_ativacoes_efetividade_analitico tbl_ativacoes
		WHERE conclusao IN ('Realizado', 'Não Realizado')
)

/* ####  Aging Altas   #### */
, tmp_ativacoes AS (
    SELECT 
        nome AS cidade,
        regional,
        cluster,
        a.unidade,
        data_primeira_ativacao as data_ativacao,
        tecnico AS tecnico_ativacao,
        COALESCE(UPPER(de_para.terceirizada_air), 'GIGA+') AS terceirizada_corrigida,
        marca,
        bairro,
        a.data_criacao,
        codigo_contrato,
        sistema_origem,
        posicao_sap
    FROM {{ get_catalogo('gold') }}.operacao.ativacao_operacao a
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u
        ON a.unidade = u.sigla AND u.fonte = 'AIR' and u.excluido = false
    LEFT JOIN (
        SELECT A.id_contrato, MAX(A.id_endereco) AS id_endereco
        FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega A 
        GROUP BY A.id_contrato
    ) AS CE 
        ON CE.id_contrato = a.codigo_contrato
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco DE 
        ON CE.id_endereco = DE.id_endereco
    LEFT JOIN {{ get_catalogo('gold') }}.operacao.tbl_depara_terceirizadas_operacao de_para
        ON a.posicao_sap = de_para.de
    WHERE 
        u.marca IS NOT NULL
        AND NOT codigo_contrato IN (SELECT codigo_contrato FROM tmp_expurgo_urbe)
)	
/* ANALÍTICO AGING ALTAS */
, tmp_analitico_aging_altas AS (
    SELECT
        DISTINCT
        posicao_sap,
        tbl_ativacoes.terceirizada_corrigida AS terceirizada,
        tbl_ativacoes.bairro,
        tbl_ativacoes.marca,
        tbl_ativacoes.cidade,
        tbl_ativacoes.regional,
        tbl_ativacoes.cluster,
        tbl_ativacoes.unidade,
        tbl_ativacoes.codigo_contrato,
        0 AS codigo_chamado,
        0 AS codigo_os,
        0 AS numero_tarefa,
        '' AS fila,
        tbl_ativacoes.tecnico_ativacao AS tecnico,
        0 AS id_tecnico,
        '' AS usuario_conclusao,
        '' AS motivo_conclusao,
        '' AS conclusao,
        tbl_ativacoes.sistema_origem,
        0 AS os_anterior,
        '' AS motivo_conclusao_anterior,
        '1900-01-01' AS data_conclusao_anterior,
        0 AS id_tecnico_anterior,
        '' AS tecnico_anterior,
        '' AS turno,
        '1900-01-01' AS data_abertura_chamado,
        '1900-01-01' AS data_abertura_os,
        '1900-01-01' AS data_inicio_servico,
        CAST(tbl_ativacoes.data_ativacao AS DATE) AS data_ativacao,
        CAST(tbl_ativacoes.data_criacao AS DATE) data_criacao_contrato,
        '1900-01-01' AS data_evento,
        '1900-01-01' AS data_conclusao,
        '' AS descricao,
        0 AS protocolo,
        DATEDIFF(DAY, tbl_ativacoes.data_criacao, tbl_ativacoes.data_ativacao)*1.0 AS aging,
        CASE WHEN 
            DATEDIFF(DAY, tbl_ativacoes.data_criacao, tbl_ativacoes.data_ativacao)  BETWEEN 0 AND 2
            AND tbl_ativacoes.data_ativacao IS NOT NULL 
                THEN 'NO PADRÃO' 
            ELSE 
                'FORA DO PADRÃO' 
        END AS aging_categoria,
        'AGING_ALTAS' AS indicador,
        trunc(tbl_ativacoes.data_ativacao, 'MM') mes_referencia
    FROM tmp_ativacoes tbl_ativacoes
)

/* ####  Reparos IFI   #### */

/* ####    TABELA DE REPAROS REALIZADOS   ##### */
, cte_reparos_realizados_IFI AS(
    SELECT 
        *
    FROM tmp_reparos
    WHERE codigo_os IN (SELECT codigo_os FROM tmp_os_reparos_realizadas)
)

/* TABELA INFÂNCIA */
, tmp_IFI_analitico AS (
    SELECT *
    FROM (
        SELECT 
            tbl_reparos.posicao_sap,
            tbl_reparos.marca,
            tbl_reparos.bairro,
            tbl_reparos.cidade,
            tbl_reparos.cluster,
            tbl_reparos.regional,
            chamado,
            codigo_os,
            fila,
            numero_tarefa,
            tbl_ativacoes.tecnico_ativacao,
            tbl_reparos.tecnico,
            tbl_reparos.id_tecnico,
            usuario_conclusao,
            motivo_conclusao,
            conclusao,
            turno,
            data_abertura_os,
            inicio_servico,
            data_ativacao,
            tbl_reparos.sistema_origem,
            data_evento,
            data_conclusao,
            descricao,
            tbl_reparos.unidade,
            tbl_ativacoes.terceirizada_corrigida AS terceirizada,
            tbl_reparos.codigo_contrato, 
            tbl_reparos.data_abertura_chamado,
            DATEDIFF(DAY, tbl_ativacoes.data_ativacao,tbl_reparos.data_abertura_chamado) AS aging,
            ROW_NUMBER() OVER( PARTITION BY tbl_ativacoes.codigo_contrato ORDER BY tbl_reparos.data_abertura_chamado ASC) AS indice_ifi
        FROM tmp_ativacoes tbl_ativacoes
        LEFT JOIN cte_reparos_realizados_IFI tbl_reparos 	
            ON (
                tbl_ativacoes.codigo_contrato = tbl_reparos.codigo_contrato
                AND DATEDIFF(DAY, tbl_ativacoes.data_ativacao,CAST(tbl_reparos.data_abertura_chamado AS DATE)) BETWEEN 0 AND 15
                )
    ) t 
    WHERE t.indice_ifi = 1 
)

/* ANALÍTICO IFI */
, tmp_final_IFI_analitico AS (
    SELECT
        DISTINCT
        posicao_sap,
        tbl_reparos.terceirizada,
        tbl_reparos.bairro,
        tbl_reparos.marca,
        tbl_reparos.cidade,
        tbl_reparos.regional,
        tbl_reparos.cluster,
        tbl_reparos.unidade,
        tbl_reparos.codigo_contrato,
        tbl_reparos.chamado AS codigo_chamado,
        tbl_reparos.codigo_os,
        tbl_reparos.numero_tarefa,
        tbl_reparos.fila,
        tbl_reparos.tecnico,
        tbl_reparos.id_tecnico,
        tbl_reparos.usuario_conclusao,
        tbl_reparos.motivo_conclusao,
        tbl_reparos.conclusao,
        tbl_reparos.sistema_origem,
        0 AS os_anterior,
        '' AS motivo_conclusao_anterior,
        '1900-01-01' AS data_conclusao_anterior,
        0 AS id_tecnico_anterior,
        tecnico_ativacao AS tecnico_anterior,
        tbl_reparos.turno,
        tbl_reparos.data_abertura_chamado,
        tbl_reparos.data_abertura_os,
        tbl_reparos.inicio_servico AS data_inicio_servico,
        tbl_reparos.data_ativacao,
        '1900-01-01' AS data_criacao_contrato,
        tbl_reparos.data_evento,
        tbl_reparos.data_conclusao,
        tbl_reparos.descricao,
        0 AS protocolo,
        DATEDIFF(DAY, tbl_reparos.data_ativacao,CAST(tbl_reparos.data_abertura_chamado AS DATE)) aging,
        '' AS aging_categoria,
        'IFI' AS indicador,
        trunc(data_evento, 'MM') mes_referencia
    FROM tmp_IFI_analitico tbl_reparos
    WHERE codigo_contrato IS NOT NULL
)

/* ####  Reparos 24h   #### */
/* ####    TABELA DE REPAROS REALIZADOS 24H   ##### */
, cte_reparos_realizados_24h AS(
    SELECT 
        *
    FROM tmp_reparos
    WHERE codigo_os IN (SELECT codigo_os FROM tmp_os_reparos_realizadas)
)


/* ANALÍTICO REPAROS 24h */
, tmp_analitico_reparos_24h AS (
    SELECT
        posicao_sap,
        tbl_reparos.terceirizada_corrigida AS terceirizada,
        tbl_reparos.bairro,
        tbl_reparos.marca,
        tbl_reparos.cidade,
        tbl_reparos.regional,
        tbl_reparos.cluster,
        tbl_reparos.unidade,
        tbl_reparos.codigo_contrato,
        tbl_reparos.chamado AS codigo_chamado,
        tbl_reparos.codigo_os,
        tbl_reparos.numero_tarefa,
        tbl_reparos.fila,
        tbl_reparos.tecnico,
        tbl_reparos.id_tecnico,
        tbl_reparos.usuario_conclusao,
        tbl_reparos.motivo_conclusao,
        tbl_reparos.conclusao,
        tbl_reparos.sistema_origem,
        0 AS os_anterior,
        '' AS motivo_conclusao_anterior,
        '1900-01-01' AS data_conclusao_anterior,
        0 AS id_tecnico_anterior,
        '' AS tecnico_anterior,
        tbl_reparos.turno,
        tbl_reparos.data_abertura_chamado,
        tbl_reparos.data_abertura_os,
        tbl_reparos.inicio_servico AS data_inicio_servico,
        '1900-01-01' AS data_ativacao,
        '1900-01-01' AS data_criacao_contrato,
        tbl_reparos.data_evento,
        tbl_reparos.data_conclusao,
        tbl_reparos.descricao,
        0 AS protocolo,
        DATEDIFF(MINUTE, tbl_reparos.data_abertura_chamado, tbl_reparos.data_conclusao) AS aging,
        CASE 
            WHEN DATEDIFF(MINUTE, tbl_reparos.data_abertura_chamado,tbl_reparos.data_conclusao) <= 1440*3/4
                THEN 'Dentro de 18 hrs'
            WHEN DATEDIFF(MINUTE, tbl_reparos.data_abertura_chamado, tbl_reparos.data_conclusao) <= 1440 
                THEN 'Dentro de 24 hrs'
            WHEN DATEDIFF(MINUTE, tbl_reparos.data_abertura_chamado, tbl_reparos.data_conclusao) <= 1440*2 
                THEN 'Dentro de 48 hrs'
            ELSE  
                'Acima de 48 hrs'
            END AS aging_categoria,
        'REPAROS_24H' AS indicador,
        trunc(data_evento, 'MM') mes_referencia
    FROM cte_reparos_realizados_24h tbl_reparos
)

/* ####  Reparos IRT   #### */
/* ####    TABELA DE REPAROS REALIZADOS IRT   ##### */
, cte_reparos_irt AS(
    SELECT 
        *,
            ROW_NUMBER() OVER(PARTITION BY chamado ORDER BY codigo_os DESC) AS os_valida
    FROM tmp_reparos
    WHERE (conclusao IS NULL OR conclusao <> 'Cancelado')
      AND  (motivo_conclusao IS NULL OR motivo_conclusao NOT IN (SELECT motivo FROM MOTIVOS_IRT)) -- Expurgo IRT
      AND ((motivo_conclusao IS NULL OR motivo_conclusao NOT IN (SELECT motivo FROM MOTIVOS_REAGENDADA) OR  conclusao <> 'Reagendado'))
)
/* TABELA DE REPAROS IRT */
, tmp_IRT_analitico AS (
    SELECT 
        posicao_sap,
        tbl_reparos.terceirizada_corrigida AS terceirizada,
        tbl_reparos.bairro,
        tbl_reparos.marca,
        tbl_reparos.cidade,
        tbl_reparos.regional,
        tbl_reparos.cluster,
        tbl_reparos.unidade,
        tbl_reparos.codigo_contrato,
        tbl_reparos.chamado AS codigo_chamado,
        tbl_reparos.codigo_os,
        tbl_reparos.numero_tarefa,
        tbl_reparos.fila,
        tbl_reparos.tecnico,
        tbl_reparos.id_tecnico,
        tbl_reparos.usuario_conclusao,
        tbl_reparos.motivo_conclusao,
        tbl_reparos.conclusao,
        tbl_reparos.sistema_origem,
        0 AS os_anterior,
        '' AS motivo_conclusao_anterior,
        '1900-01-01' AS data_conclusao_anterior,
        0 AS id_tecnico_anterior,
        '' AS tecnico_anterior,
        tbl_reparos.turno,
        tbl_reparos.data_abertura_chamado,
        tbl_reparos.data_abertura_os,
        tbl_reparos.inicio_servico AS data_inicio_servico,
        '1900-01-01' AS data_ativacao,
        '1900-01-01' AS data_criacao_contrato,
        tbl_reparos.data_evento,
        tbl_reparos.data_conclusao,
        tbl_reparos.descricao,
        tbl_reparos.chamado AS protocolo,
        0 AS aging,
        'Sem categoria' AS aging_categoria,
        'IRT' AS indicador,
        trunc(data_evento, 'MM') mes_referencia
    FROM cte_reparos_irt tbl_reparos
    WHERE 1=1
    AND (
        (
            tbl_reparos.os_valida = 1
            AND NOT EXISTS(
                SELECT 1
                FROM tmp_os_reparos_realizadas b
                WHERE tbl_reparos.chamado = b.chamado
            )
        )
        OR EXISTS(
            SELECT 1
            FROM tmp_os_reparos_realizadas c
            WHERE tbl_reparos.codigo_os = c.codigo_os
        )
    )
)

/* ####  Reparos IRR   #### */
/* ####    TABELA DE REPAROS REALIZADOS IRR   ##### */
, cte_reparos_realizados_IRR AS(
    SELECT 
        *
    FROM tmp_reparos
    WHERE codigo_os IN (SELECT codigo_os FROM tmp_os_reparos_realizadas)
)

/* TABELA REPETIDAS */
, tmp_repetidas_analitico AS (
    SELECT 
        *
    FROM(
        SELECT
            chamado AS chamado_atual, 
            LAG (chamado,1,0) OVER (ORDER BY codigo_contrato, data_abertura_chamado, chamado) AS chamado_anterior,
            data_conclusao AS data_conclusao_atual,
            LAG (data_conclusao,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS data_conclusao_anterior,
            data_abertura_chamado AS data_abertura_atual,
            LAG (data_abertura_chamado,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS data_abertura_anterior,
            codigo_contrato AS codigo_contrato_atual,
            LAG (codigo_contrato,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS codigo_contrato_anterior,
            conclusao AS conclusao_atual,
            LAG (conclusao,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS conclusao_anterior,
            motivo_conclusao AS motivo_conclusao_atual,
            LAG (motivo_conclusao,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS motivo_conclusao_anterior,
            unidade AS unidade_atual,
            LAG (unidade,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS unidade_anterior,
            terceirizada_corrigida AS terceirizada_atual,
            LAG (terceirizada_corrigida,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS terceirizada_anterior,
            id_tecnico AS id_tecnico_atual,
            LAG (id_tecnico,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS id_tecnico_anterior,
            tecnico AS tecnico_atual,
            LAG (tecnico,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS tecnico_anterior,
            codigo_os AS os_atual, 
            LAG (codigo_os,1,0) OVER (ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS os_anterior,
            RANK () OVER (PARTITION BY codigo_contrato ORDER BY codigo_contrato,data_abertura_chamado, chamado) AS indice_repetida
        FROM cte_reparos_realizados_IRR 
    ) as tbl_repetida 
    WHERE 
        tbl_repetida.indice_repetida <> 1
        AND DATEDIFF(DAY, to_date(tbl_repetida.data_conclusao_anterior), to_date(tbl_repetida.data_abertura_atual)) <= 30
        AND tbl_repetida.codigo_contrato_anterior = tbl_repetida.codigo_contrato_atual
        AND tbl_repetida.chamado_atual <> tbl_repetida.chamado_anterior
        AND tbl_repetida.data_abertura_atual > tbl_repetida.data_abertura_anterior
        AND tbl_repetida.data_conclusao_atual > tbl_repetida.data_conclusao_anterior
)

    /* ANALÍTICO REPAROS IRR */
, tmp_analitico_IRR AS (
    SELECT
        posicao_sap,
        COALESCE(tbl_repetidas.terceirizada_anterior,tbl_reparos.terceirizada_corrigida) terceirizada,
        tbl_reparos.bairro,
        tbl_reparos.marca,
        tbl_reparos.cidade,
        tbl_reparos.regional,
        tbl_reparos.cluster,
        tbl_reparos.unidade,
        tbl_reparos.codigo_contrato,
        tbl_reparos.chamado AS codigo_chamado,
        tbl_reparos.codigo_os,
        0 AS numero_tarefa,
        tbl_reparos.fila,
        tbl_reparos.tecnico,
        tbl_reparos.id_tecnico,
        '' AS usuario_conclusao,
        tbl_reparos.motivo_conclusao,
        '' AS conclusao,
        '' AS sistema_origem,
        os_anterior,
        tbl_repetidas.motivo_conclusao_anterior,
        tbl_repetidas.data_conclusao_anterior,
        tbl_repetidas.id_tecnico_anterior,
        CONCAT(tbl_repetidas.tecnico_anterior,' - ',tbl_repetidas.terceirizada_anterior) tecnico_anterior,
        '' AS turno,
        tbl_reparos.data_abertura_chamado,
        tbl_reparos.data_abertura_os,
        '1900-01-01' AS data_inicio_servico,
        '1900-01-01' AS data_ativacao,
        '1900-01-01' AS data_criacao_contrato,
        tbl_reparos.data_evento,
        tbl_reparos.data_conclusao,
        '' AS descricao,
        tbl_reparos.chamado AS protocolo,
        DATEDIFF(DAY, tbl_repetidas.data_conclusao_anterior, tbl_repetidas.data_abertura_atual) AS aging,
        '' AS aging_categoria,
        'IRR' AS indicador,
        trunc(data_evento, 'MM') mes_referencia
    FROM (
        SELECT * 
        FROM cte_reparos_realizados_IRR 
    ) tbl_reparos
    LEFT JOIN tmp_repetidas_analitico tbl_repetidas
        ON (tbl_reparos.chamado = tbl_repetidas.chamado_atual)
)


, tbl_indicadores AS (
    SELECT 
        DISTINCT *
        FROM (
            (SELECT * FROM tmp_analitico_mudanca)
            UNION
            (SELECT * FROM tmp_analitico_efetividade)
            UNION
            (SELECT * FROM tmp_analitico_aging_altas)
            UNION
            (SELECT * FROM tmp_final_IFI_analitico)
            UNION
            (SELECT * FROM tmp_analitico_reparos_24h)
            UNION
            (SELECT * FROM tmp_IRT_analitico)
            UNION
            (SELECT * FROM tmp_analitico_IRR)
        ) all_data
)
SELECT *
FROM tbl_indicadores
