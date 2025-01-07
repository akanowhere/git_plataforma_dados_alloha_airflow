WITH temp_dim_pacotes_ott AS (
    SELECT nome_pacote,
           nome_ott,
           possui_ott
    FROM (
        SELECT nome_pacote,
               CASE
                    WHEN TRIM(UPPER(REPLACE(nome_pacote, ' ', ''))) LIKE '%GLOBOPLAY%' AND nome_pacote NOT LIKE '%GLOBOPLAY+CANAIS%' THEN 1
                    ELSE 0
               END AS globoplay,
               CASE
                    WHEN TRIM(UPPER(REPLACE(nome_pacote, ' ', '')))  LIKE '%GLOBOPLAY+CANAIS%' THEN 1
                    ELSE 0
               END AS globoplay_canais,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%PREMIERE%' THEN 1
                    ELSE 0
               END AS premiere,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%TELECINE%' THEN 1
                    ELSE 0
               END AS telecine,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%HBO%' THEN 1
                    ELSE 0
               END AS hbo,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%PARAMOUNT%' THEN 1
                    ELSE 0
               END AS paramount,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%SKY%' THEN 1
                    ELSE 0
               END AS sky,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%DGO%' THEN 1
                    ELSE 0
               END AS dgo,
               CASE
                    WHEN UPPER(nome_pacote)  LIKE '%DIRECTV%' THEN 1
                    ELSE 0
               END AS directvgo,
               CASE
                    WHEN TRIM(UPPER(REPLACE(nome_pacote, ' ', '')))  LIKE '%GIGA+TVBASICO%'  THEN 1
                    ELSE 0
               END AS giga_mais_basico,
               CASE
                    WHEN TRIM(UPPER(REPLACE(nome_pacote, ' ', '')))  LIKE '%GIGA+TVFAMILIA%' THEN 1
                    ELSE 0
               END AS giga_mais_familia,
               CASE
                    WHEN TRIM(UPPER(REPLACE(nome_pacote, ' ', '')))  LIKE '%GIGA+TVCINEMA%' THEN 1
                    ELSE 0
               END AS giga_mais_cinema
        FROM (
            SELECT DISTINCT UPPER(pacote_nome) AS nome_pacote 
            FROM gold.base.dim_contrato
            UNION 
            SELECT DISTINCT UPPER(antigo_pacote_nome) AS pacote_nome
            FROM gold.base.dim_migracao 
        ) A
    ) B
    UNPIVOT (
        possui_ott FOR nome_ott IN (globoplay,
                                    globoplay_canais,
                                    premiere,
                                    telecine,
                                    hbo,
                                    paramount,
                                    sky,
                                    dgo,
                                    directvgo,
                                    giga_mais_basico,
                                    giga_mais_familia,
                                    giga_mais_cinema)
    ) unpiv
)
,tmp_planos AS (
     SELECT id_contrato,pacote_nome
     FROM gold.base.dim_contrato
     UNION ALL
     SELECT id_contrato,antigo_pacote_nome AS pacote_nome
     FROM gold.base.dim_migracao     
)    
,tmp_contratos_ott AS (
    SELECT DISTINCT t1.id_contrato, nome_ott
    FROM tmp_planos t1
    INNER JOIN temp_dim_pacotes_ott t2 ON (UPPER(t1.pacote_nome) = UPPER(t2.nome_pacote) AND t2.possui_ott = 1)
)
,tmp_contrato_mes_atual AS (
    SELECT DISTINCT 
        t1.id_contrato,
        t1.data_primeira_ativacao,
        t1.data_cancelamento,
        t1.unidade_atendimento,
        t1.pacote_nome,
        t1.legado_sistema,
        t1.status AS status_contrato,
        t1.data_criacao
    FROM gold.base.dim_contrato t1
    INNER JOIN tmp_contratos_ott t2 ON (t1.id_contrato = t2.id_contrato)
)
,tmp_contrato_mes_passado AS (
    SELECT DISTINCT 
        t1.id_contrato,
        t1.pacote_nome,
        t1.status_FM AS status_contrato,
        t1.data_referencia
    FROM gold.relatorios.tbl_contratos_ott t1
    WHERE t1.data_referencia = CAST(date_format(current_date()-1, 'yyyy-MM-01') AS DATE)-1
)
,cte_contratos_ott AS (
    SELECT 
        t1.id_contrato,
        CAST(t1.data_criacao AS DATE) AS data_criacao,
        CAST(t1.data_primeira_ativacao AS DATE) AS data_primeira_ativacao,
        CAST(t1.data_cancelamento AS DATE) AS data_cancelamento,
        CAST(date_format(current_date()-1, 'yyyy-MM-01') AS DATE) AS intervalo,
        t1.unidade_atendimento,
        CASE WHEN t1.legado_sistema IS NOT NULL THEN 1 ELSE 0 END AS tombamento,
        t1.pacote_nome,
        t2.pacote_nome AS pacote_nome_MP,
        t3.nome_ott,
        t3.possui_ott AS possui_ott_FM,
        COALESCE(t5.possui_ott, 0) AS possui_ott_MP,
        t1.status_contrato AS status_FM,
        t2.status_contrato AS status_MP,
        CASE WHEN t1.status_contrato IN ('ST_CONT_SUSP_DEBITO','ST_CONT_SUSP_SOLICITACAO','ST_CONT_SUSP_CANCELAMENTO') THEN 1 ELSE 0 END AS flg_suspenso_FM,
        CASE WHEN t2.status_contrato IN ('ST_CONT_SUSP_DEBITO','ST_CONT_SUSP_SOLICITACAO','ST_CONT_SUSP_CANCELAMENTO') THEN 1 ELSE 0 END AS flg_suspenso_MP,
        CURRENT_DATE()-1 AS data_referencia
    FROM tmp_contrato_mes_atual t1
    LEFT JOIN tmp_contrato_mes_passado t2 ON (t1.id_contrato = t2.id_contrato AND t2.status_contrato IN ('ST_CONT_HABILITADO', 'ST_CONT_SUSP_DEBITO', 'ST_CONT_SUSP_SOLICITACAO', 'ST_CONT_SUSP_CANCELAMENTO'))
    INNER JOIN temp_dim_pacotes_ott t3 ON (UPPER(t1.pacote_nome) = UPPER(t3.nome_pacote))
    INNER JOIN tmp_contratos_ott t4 ON (t1.id_contrato = t4.id_contrato AND t3.nome_ott = t4.nome_ott)
    LEFT JOIN temp_dim_pacotes_ott t5 ON (UPPER(t2.pacote_nome) = UPPER(t5.nome_pacote))
    WHERE  t1.data_primeira_ativacao IS NOT NULL
        AND (t1.status_contrato IN ('ST_CONT_HABILITADO', 'ST_CONT_SUSP_DEBITO', 'ST_CONT_SUSP_SOLICITACAO', 'ST_CONT_SUSP_CANCELAMENTO') 
            OR t1.status_contrato = 'ST_CONT_CANCELADO' AND date_format(t1.data_cancelamento, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01'))
        AND (t3.nome_ott = t5.nome_ott OR t2.pacote_nome IS NULL)
)
SELECT  id_contrato,
        data_criacao,
        data_primeira_ativacao,
        data_cancelamento,
        intervalo,
        unidade_atendimento,
        tombamento,
        pacote_nome,
        pacote_nome_MP,
        nome_ott,
        possui_ott_FM,
        possui_ott_MP,
        status_FM,
        status_MP,
        flg_suspenso_FM,
        flg_suspenso_MP,
        data_referencia
FROM cte_contratos_ott
UNION ALL
SELECT  t1.id_contrato,
        t1.data_criacao,
        t1.data_primeira_ativacao,
        t2.data_cancelamento,
        t1.intervalo,
        t1.unidade_atendimento,
        t1.tombamento,
        t1.pacote_nome,
        t1.pacote_nome_MP,
        t1.nome_ott,
        t1.possui_ott_FM,
        t1.possui_ott_MP,
        t1.status_FM,
        t1.status_MP,
        t1.flg_suspenso_FM,
        t1.flg_suspenso_MP,
        t1.data_referencia
FROM {{ this }} t1
LEFT JOIN gold.base.dim_contrato t2 ON t1.id_contrato = t2.id_contrato
WHERE intervalo <> CAST(date_format(current_date()-1, 'yyyy-MM-01') AS DATE);


