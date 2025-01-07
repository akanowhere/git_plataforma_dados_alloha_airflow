{{ config(
    materialized='incremental',
    unique_key=['data_referencia']
) }}
/*###############################*/
/* ####     BONIFICAÇÃO     #### */
/*###############################*/
WITH data_referencia AS (
    SELECT CAST(DATE_SUB(current_date(),1) AS DATE) AS DATA_REFERENCIA
)

/* Geral regional/terceirizada/indicador */
, cte_emp AS (
    SELECT 
        id_usuario, 
        regionais, 
        data_referencia,
        ROW_NUMBER() OVER(PARTITION BY id_usuario ORDER BY data_referencia DESC) ord
    FROM {{ get_catalogo('gold') }}.operacao.tbl_cadastro_terceirizadas
    WHERE data_referencia < (SELECT DATA_REFERENCIA FROM data_referencia)
)

, tmp_geral AS (
    SELECT DISTINCT 
        u.regional, 
        t.terceirizada, 
        v.indicador
    FROM {{ get_catalogo('gold') }}.base.dim_unidade u
    LEFT JOIN (
        SELECT 
            terceirizada, 
            REPLACE(REPLACE(REPLACE(REPLACE(regionais,'`',''),'',''),'',''),',','') AS regionais
        FROM {{ get_catalogo('gold') }}.operacao.tbl_cadastro_terceirizadas 
        WHERE regionais <> '' 
            AND data_referencia IS NOT NULL
    ) t ON t.regionais LIKE CONCAT('%',u.regional,'%')
    CROSS JOIN
        (VALUES ('IFI'), ('IRT'), ('IRR'),('REPAROS_24H') ,('AGENDAMENTO'),('ATINGIMENTO_DE_ALTAS'),('AGING_ALTAS')) AS v(indicador)
    WHERE u.excluido = false 
        AND t.terceirizada IS NOT NULL
)
, tbl_resultado AS (
    SELECT
        tbl_ativacoes.regional,
        tbl_ativacoes.terceirizada, 
        'AGENDAMENTO' AS indicador,
        COALESCE(tpeso.peso,0) AS peso,
        80 AS resultado,
        (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
    FROM  tmp_geral tbl_ativacoes
    LEFT JOIN (
            SELECT 
                terceirizada, 
                regional, 
                indicador, 
                peso  
            FROM(
                SELECT 
                    replace(regional,'EGIONAL ','') AS regional,
                    t1.empresa AS terceirizada, 
                    IFI,
                    IRR,
                    IRT,
                    AGENDAMENTO, 
                    REPAROS_24H, 
                    ATINGIMENTO_DE_ALTAS,
                    AGING_ALTAS
                FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao t1
                LEFT JOIN (
                        SELECT empresa, MAX(data_referencia) data  
                        FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao
                        GROUP BY empresa
                    ) t2 ON t1.empresa = t2.empresa AND t1.data_referencia = t2.data
                    WHERE data is not null
            ) p  
            UNPIVOT  
                (peso FOR indicador IN   
                    (IFI,IRR,IRT,AGENDAMENTO, REPAROS_24H, ATINGIMENTO_DE_ALTAS, AGING_ALTAS)  
            )AS unpv
    ) tpeso ON tbl_ativacoes.terceirizada = tpeso.terceirizada and tbl_ativacoes.regional = tpeso.regional and tpeso.indicador = 'AGENDAMENTO'
    UNION
    (
        SELECT
            g.regional, 
            g.terceirizada, 
            g.indicador, 
            p.peso,
            CAST(ROUND((SUM(metrica1)*1.0)/(SUM(CASE WHEN  (metrica2 = 0 OR metrica2 is null) THEN 0.000001 ELSE metrica2 END)*1.0), 4)*100  AS DECIMAL(20, 2)) AS resultado,
            (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
        FROM tmp_geral g
        LEFT JOIN  {{ get_catalogo('gold') }}.operacao.tbl_indicadores_operacao i 
            ON (g.indicador = i.indicador 
                AND g.terceirizada = i.Terceirizada 
                AND g.regional = i.regional 
                AND i.data_referencia = (SELECT DATA_REFERENCIA FROM data_referencia) 
            )
        LEFT JOIN(
            SELECT 
            empresa, 
            regional, 
            indicador, 
            peso
            FROM (
                SELECT 
                empresa, 
                regional, 
                IFI, 
                IRR, 
                IRT, 
                AGENDAMENTO, 
                ATINGIMENTO_DE_ALTAS, 
                REPAROS_24H, 
                AGING_ALTAS
                FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao
                WHERE data_referencia = (
                    SELECT MAX(data_referencia)
                    FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao
                )
            ) AS p
            UNPIVOT (
                peso FOR indicador IN (IFI, IRR, IRT, AGENDAMENTO, ATINGIMENTO_DE_ALTAS, REPAROS_24H,AGING_ALTAS)
            ) AS unpvt
        ) AS p 
        ON (p.regional = g.regional 
            AND p.empresa = g.terceirizada 
            AND p.indicador = g.indicador
        )
        WHERE 
            g.indicador NOT IN ('AGENDAMENTO','ATINGIMENTO_DE_ALTAS')
        GROUP BY
            g.regional, g.terceirizada, g.indicador, p.peso
    )
    UNION
    (
        SELECT 
            g.regional, 
            g.terceirizada, 
            g.indicador, p.peso, 
            ROUND((((COUNT(DISTINCT a.codigo_contrato))+(COUNT(DISTINCT a.codigo_contrato)/f.fator_inicio)*f.fator_fim)/m.meta)*100,2) AS resultado,
            (SELECT DATA_REFERENCIA FROM data_referencia) AS data_referencia
        FROM tmp_geral AS g
        LEFT JOIN (
            SELECT 
            a.terceirizada, 
            a.codigo_contrato, 
            u.regional
            FROM {{ get_catalogo('gold') }}.operacao.ativacao_operacao a
            LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
                ON (a.unidade = u.sigla)
            WHERE DATE_FORMAT(a.data_primeira_ativacao, 'yyyy-MM') = DATE_FORMAT((SELECT DATA_REFERENCIA FROM data_referencia),'yyyy-MM')
                AND a.data_ativacao_evento <= (SELECT DATA_REFERENCIA FROM data_referencia)
        ) AS a
            ON (g.regional = a.regional AND g.terceirizada = a.terceirizada AND g.indicador = 'ATINGIMENTO_DE_ALTAS')
        LEFT JOIN (
                SELECT 
                ROUND(SUM(CASE WHEN data <= (SELECT DATA_REFERENCIA FROM data_referencia)  THEN fator ELSE 0 END),2) AS fator_inicio,
                ROUND(SUM(CASE WHEN data > (SELECT DATA_REFERENCIA FROM data_referencia)  THEN fator ELSE 0 END),2) AS fator_fim
            FROM {{ get_catalogo('gold') }}.venda_hora.seed_fator_vendas
            WHERE DATE_FORMAT(data, 'yyyy-MM') = DATE_FORMAT((SELECT DATA_REFERENCIA FROM data_referencia), 'yyyy-MM')
        ) AS f 
        ON (1=1)
        LEFT JOIN(
            SELECT 
            empresa, 
            regional, 
            indicador, 
            peso
            FROM (
                SELECT 
                empresa, 
                regional, 
                IFI, 
                IRR, 
                IRT, 
                AGENDAMENTO, 
                ATINGIMENTO_DE_ALTAS, 
                REPAROS_24H,AGING_ALTAS
                FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao
                    WHERE data_referencia = (
                    SELECT DISTINCT 
                    MAX(data_referencia)
                    FROM {{ get_catalogo('gold') }}.operacao.tbl_pesos_bonificacao
                    WHERE data_referencia < (SELECT DATA_REFERENCIA FROM data_referencia)
                )
            ) AS p
            UNPIVOT (
                peso FOR indicador IN (IFI, IRR, IRT, AGENDAMENTO, ATINGIMENTO_DE_ALTAS, REPAROS_24H,AGING_ALTAS)
            ) AS unpvt
        ) AS p 
        ON (p.regional = g.regional 
            AND p.empresa = g.terceirizada 
            AND p.indicador = g.indicador
        )
        LEFT JOIN (
            SELECT 
            t1.*
            FROM {{ get_catalogo('gold') }}.operacao.tbl_meta_atingimento_altas t1
            LEFT JOIN (
                    SELECT 
                    empresa, 
                    MAX(data_referencia) data  
                    FROM {{ get_catalogo('gold') }}.operacao.tbl_meta_atingimento_altas
                    GROUP BY empresa
            ) t2 
            ON t1.empresa = t2.empresa AND t1.data_referencia = t2.data
            WHERE data is not null
        ) AS m
            ON (g.terceirizada = m.empresa AND g.regional = m.regional)
        WHERE g.indicador = 'ATINGIMENTO_DE_ALTAS'
        GROUP BY
            g.regional, g.terceirizada, g.indicador, f.fator_inicio, f.fator_fim, p.peso, m.meta
        )
)
SELECT 
    regional,
    Terceirizada,
    indicador,
    peso,
    resultado,
    CASE WHEN polaridade = 'up'
        THEN
            CASE 
                WHEN resultado >= limite_superior THEN peso*1
                WHEN resultado < limite_inferior THEN peso*0.6
                ELSE peso*0.8
            END
        ELSE
            CASE 
                WHEN resultado <= limite_superior THEN peso*1
                WHEN resultado > limite_inferior THEN peso*0.6
                ELSE peso*0.8
            END
    END pontuacao,
    data_referencia
FROM tbl_resultado t1
LEFT JOIN (
    SELECT 
        b.sigla,
        meta,
        polaridade,
        limite_superior,
        limite_inferior
    FROM (
        SELECT 
            sigla, 
            MAX(data_referencia) data_referencia
        FROM {{ get_catalogo('gold') }}.operacao.tbl_informacoes_indicadores
        GROUP BY sigla
    ) a
    LEFT JOIN {{ get_catalogo('gold') }}.operacao.tbl_informacoes_indicadores b
        ON a.sigla = b.sigla and a.data_referencia = b.data_referencia
) t2 ON t1.indicador = t2.sigla
WHERE t1.peso IS NOT NULL
