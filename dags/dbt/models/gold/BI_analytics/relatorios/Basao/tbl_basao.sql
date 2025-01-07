WITH cte_fidelizados AS (
    SELECT DISTINCT
        dc.codigo_externo AS CONTRATO_CODIGO_AIR, 
        1 AS ind_fidelizacao_atual,
        MAX(CAST(dcd.data_fim_fidelidade AS DATE)) AS data_vencimento_fidelidade
    FROM gold.sydle.dim_contrato_sydle dc
    LEFT JOIN gold.sydle.dim_contrato_descontos dcd
        ON dc.id_contrato = dcd.id_contrato
    WHERE (produto_nome = 'INSTALAÇÃO FIBRA'  OR produto_nome = 'instalacao fibra')
      AND desconto_nome LIKE '%multa de fidelidade%'
      AND CAST(dcd.data_fim_fidelidade AS DATE) > CURRENT_DATE()-1
    GROUP BY dc.codigo_externo
)
, cte_reajuste AS (
    SELECT 
        id_contrato,
        data_processamento,
        valor_anterior,
        porcentagem_reajuste AS valor_indice,
        valor_final,
        ROW_NUMBER() OVER (PARTITION BY id_contrato ORDER BY data_processamento DESC) AS rn
    FROM gold.base.dim_contrato_reajuste
)
, cte_internet AS (
    SELECT 
        id_login,
        contrato_codigo,
        onu_serial,
        onu_modelo,
        habilitado,
        slot,
        pon,
        roteador_serial,
        fabricante_onu,
        qtd_portas_lan_onu,
        qtd_portas_telefonia_onu,
        possui_wifi_onu,
        porta_giga_onu,
        wifi_onu,
        descricao_banda,
        ROW_NUMBER() OVER (PARTITION BY contrato_codigo ORDER BY id_login DESC) AS rn
    FROM gold.base.dim_conexao
)
, cte_campanha AS (
    SELECT 
        t1.id_contrato,
        t2.nome,
        t2.codigo,
        ROW_NUMBER() OVER (PARTITION BY id_contrato ORDER BY id_contrato_campanha DESC) AS rn
    FROM gold.base.dim_contrato_campanha t1
    LEFT JOIN gold.base.dim_campanha t2
        ON t1.id_campanha = t2.id_campanha
)
, cte_endereco_entrega AS (
    SELECT 
        id_contrato,
        id_endereco,
        ROW_NUMBER() OVER (PARTITION BY id_contrato ORDER BY id_endereco DESC) AS rn
    FROM gold.base.dim_contrato_entrega
)
,cte_desconto_trat AS (
    SELECT t1.*,
        ROW_NUMBER() OVER (PARTITION BY t1.id_contrato ORDER BY t1.pacote_atual DESC,data_ultimo_desconto DESC, t1.desconto DESC) AS rn_ult_desc,
        ROW_NUMBER() OVER (PARTITION BY t1.id_contrato,t1.categoria ORDER BY t1.pacote_atual DESC,t1.data_ultimo_desconto DESC, t1.desconto DESC) AS rn_ult_desc_cat
    FROM (  
        SELECT  t1.id,
                t1.id_contrato,
                t1.item_codigo,
                t1.data_criacao,
                t1.data_validade,
                t1.desconto,
                REPLACE(UPPER(t1.categoria),'_',' ') AS categoria,
                t2.data_final As data_migracao,
                CASE
                    WHEN UPPER(t1.item_codigo) = UPPER(t3.pacote_codigo) AND UPPER(t3.pacote_codigo) IS NOT NULL THEN t1.data_validade 
                    ELSE  LEAST(CAST(t1.data_validade AS DATE),CAST(t2.data_final AS DATE))
                END data_ultimo_desconto,
                CASE
                    WHEN UPPER(t1.item_codigo) = UPPER(t3.pacote_codigo)  THEN 1 
                    ELSE 0
                END pacote_atual
        FROM gold.base.dim_desconto t1
        LEFT JOIN (
                SELECT id_contrato,
                    pacote_base_codigo,
                    pacote_base_nome,
                    CAST(data_criacao AS TIMESTAMP) AS data_criacao,
                    CAST(LEAD(data_criacao) OVER ( PARTITION BY id_contrato ORDER BY data_criacao ) AS TIMESTAMP) AS data_final
                FROM gold.venda.dim_venda_geral
                WHERE excluido = false AND natureza in ('VN_MIGRACAO_UP','VN_MIGRACAO_COMPULSORIA','VN_NOVO_CONTRATO','VN_MIGRACAO_DOWN') and id_contrato is not null 
                ORDER BY id_contrato DESC, id_venda
        ) t2 ON t1.id_contrato = t2.id_contrato AND UPPER(t1.item_codigo) = UPPER(t2.pacote_base_codigo) --AND CAST(t1.data_criacao AS TIMESTAMP) > t2.data_criacao AND CAST(t1.data_criacao AS TIMESTAMP) < COALESCE(t2.data_final,CURRENT_TIMESTAMP()))
        LEFT JOIN gold.base.dim_contrato t3 ON (t1.id_contrato = t3.id_contrato)
        WHERE  REPLACE(UPPER(t1.categoria),'_',' ') IN ('CAMPANHA', 'CORTESIA', 'AJUSTE COMERCIAL', 'RETENCAO', 'AJUSTE FINANCEIRO') 
                AND t1.excluido = false      
                AND t1.desconto > 0 
                --AND t1.id_contrato = 5521006
        ) t1
)
,cte_desconto_categorias AS (
    SELECT id_contrato,
        categoria,
        pacote_atual,
        CASE
                WHEN pacote_atual = 1 AND data_ultimo_desconto >= CURRENT_DATE()-1 THEN 1
                else 0
        END ativo,
        CASE
            WHEN rn_ult_desc = 1 THEN 1
            ELSE 0
        END ult_desc,
        desconto,
        data_ultimo_desconto

    FROM cte_desconto_trat
    WHERE rn_ult_desc_cat = 1
)
, cte_ultima_ativacao AS (
    SELECT id_contrato, max(data_venda) as ultima_data_ativacao
    FROM gold.base.dim_migracao
    WHERE reprovada = false
    GROUP BY id_contrato
)
, cte_basao AS (
    SELECT 
        t1.id_contrato AS codigo_contrato_air,
        t1.id_cliente AS codigo_cliente,
        CASE 
            WHEN t1.id_cliente in (2191310,1302391,1302380,1302363,1302363,1302348,1302348,1302248,1302243,1194405,819068
                                    ,801446,730955,622043,610585,606439,245610,174593,110267,108855,93512,90321,78106,42305
                                    ,2860,2629,2547,2181,2146,2123,2065,1643,1638,1636,1630,1375,1358,1184,1181,335,292,275,265
                                    ,232,131,22,20) THEN 1
            ELSE 0
        END AS flag_cnpj_sumicity,
        CAST(t1.data_primeira_ativacao AS DATE) AS data_ativacao_contrato,
        GREATEST(CAST(t12.ultima_data_ativacao AS DATE),CAST(t1.data_primeira_ativacao AS DATE)) AS data_ativacao_pacote_atual,
        t13.data_venda,
        REPLACE(t1.status, 'ST_CONT_', '') AS status_contrato,
        t6.tipo AS tipo_pessoa,
        CASE
        WHEN t1.pacote_nome  like '%URBE%' THEN 'B2C'
            WHEN t1.b2b = true THEN 'B2B'
            WHEN t1.pme = true THEN 'PME'
            WHEN t6.tipo = 'JURIDICO' THEN 'PME_LEGADO'
            ELSE 'B2C'
        END AS segmento,
        CASE
            WHEN UPPER(t1.pacote_nome) LIKE ANY (
            '%MBPS%', '%MEGA%', '%MB%','%KBPS%','%GBPS%','%GIGA%','%GB%',
            '%LINK%','%INTERLIGAÇÃO%','%LAN-TO-LAN%','%L2L%','%SCM%'
            ) 
            THEN 'BL'
            ELSE '  '
        END AS flag_bl,
        CASE
            WHEN UPPER(t1.pacote_nome) LIKE ANY (
            '%FIXO%', '%TEL%', '%FALE%','%NÚMEROS%','%MINUTO%','%VOZ%','%FX%')
            THEN 'FX'
            ELSE '  '
        END AS flag_fx,
        CASE
            WHEN UPPER(t1.pacote_nome) LIKE ANY ('%LIVRE%', '%BRONZE%', '%PRATA%','%OURO%')
            THEN 'TV'
            ELSE '  '
        END AS flag_tv,
        t4.descricao_banda AS velocidade_internet,
        t5.nome AS nome_campanha,
        t5.codigo AS codigo_campanha,
        t2.data_vencimento_fidelidade,
        t2.ind_fidelizacao_atual AS flag_fidelizacao_atual,
        t1.pacote_nome AS nome_pacote,
        t1.pacote_codigo AS codigo_pacote,
        t1.valor_final AS valor_total,
        ROUND(t1.valor_padrao_plano,2) AS valor_soma_itens,
        ROUND(t1.valor_adicionais,2) AS valor_soma_adicionais,
        t1.adicional_desc AS descricao_adicionais,
        MAX(CASE
            WHEN t10.categoria = 'CAMPANHA' AND t10.ativo = 1 THEN ROUND(t10.desconto,2)
            ELSE 0
        END) desconto_campanha,
        MAX(CASE
            WHEN t10.categoria = 'CAMPANHA' AND t10.pacote_atual = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto_campanha,
        MAX(CASE
            WHEN t10.categoria = 'RETENCAO' AND t10.ativo = 1 THEN ROUND(t10.desconto,2)
            ELSE 0
        END) desconto_retencao,
        MAX(CASE
            WHEN t10.categoria = 'RETENCAO' AND t10.pacote_atual = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto_retencao,
        MAX(CASE
            WHEN t10.categoria = 'AJUSTE COMERCIAL' AND t10.ativo = 1 THEN ROUND(t10.desconto,2)
            ELSE 0
        END) desconto_ajuste_comercial,
        MAX(CASE
            WHEN t10.categoria = 'AJUSTE COMERCIAL' AND t10.pacote_atual = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto_ajuste_comercial,
        MAX(CASE
            WHEN t10.categoria = 'AJUSTE FINANCEIRO' AND t10.ativo = 1 THEN ROUND(t10.desconto,2)
            ELSE 0
        END) desconto_ajuste_financeiro,
        MAX(CASE
            WHEN t10.categoria = 'AJUSTE FINANCEIRO' AND t10.pacote_atual = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto_ajuste_financeiro,
        MAX(CASE
            WHEN t10.categoria = 'CORTESIA' AND t10.ativo = 1 THEN ROUND(t10.desconto,2)
            ELSE 0
        END) desconto_cortesia,
        MAX(CASE
            WHEN t10.categoria = 'CORTESIA' AND t10.pacote_atual = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto_cortesia,
        SUM(CASE
                WHEN t10.ativo = 1 THEN ROUND(t10.desconto,2)
                ELSE 0
        END) AS desconto_total,       
        MAX(CASE
            WHEN ult_desc = 1 THEN t10.data_ultimo_desconto
            ELSE NULL
        END) data_ultimo_desconto,
        t4.fabricante_onu AS fabricante,
        t4.qtd_portas_lan_onu AS quantidade_portas_lan,
        t4.qtd_portas_telefonia_onu AS quantidade_portas_telefonia,
        t4.possui_wifi_onu AS flag_wifi,
        t4.porta_giga_onu AS flag_giga,
        t4.wifi_onu AS tipo_wifi,
        t4.onu_serial AS serial_onu,
        t4.onu_modelo AS modelo_onu,
        t1.marcador AS marcador_contrato,
        t6.marcador AS marcador_cliente,
        CAST(t3.data_processamento AS DATE) AS data_ultimo_reajuste,
        t3.valor_anterior AS ticket_antes_reajuste,
        t3.valor_final AS ticket_apos_reajuste,
        t3.valor_indice AS aliquota_reajuste,
        coalesce(t9.marca,'S.I.')  AS marca,
        t9.cluster,
        t9.regional,
        t8.estado AS uf,
        t8.unidade,
        t8.cidade,
        t8.cep,
        t8.logradouro,
        t8.bairro,
        t8.complemento,
        t8.numero,
        CURRENT_DATE()-1 as data_referencia,
        date_format(CURRENT_DATE()-1, 'yyyy-MM') as mes_referencia,
        CURRENT_DATE() AS data_atualizacao
FROM gold.base.dim_contrato t1
LEFT JOIN cte_fidelizados t2
    ON t1.id_contrato = t2.contrato_codigo_air
LEFT JOIN cte_reajuste t3
    ON t1.id_contrato = t3.id_contrato AND t3.rn = 1
LEFT JOIN cte_internet t4
    ON t1.id_contrato = t4.contrato_codigo AND t4.rn = 1
LEFT JOIN cte_campanha t5
    ON t1.id_contrato = t5.id_contrato AND t5.rn = 1
LEFT JOIN gold.base.dim_cliente t6
    ON t1.id_cliente = t6.id_cliente AND t6.fonte = 'AIR'
LEFT JOIN cte_endereco_entrega t7
    ON t1.id_contrato = t7.id_contrato AND t7.rn = 1
LEFT JOIN gold.base.dim_endereco t8
    ON t7.id_endereco = t8.id_endereco AND t8.fonte = 'AIR'
LEFT JOIN gold.base.dim_unidade t9
    ON t8.unidade = t9.sigla AND t9.excluido = false AND t9.fonte = 'AIR'
LEFT JOIN cte_desconto_categorias t10  ON t1.id_contrato = t10.id_contrato
LEFT JOIN cte_ultima_ativacao t12
    ON t1.id_contrato = t12.id_contrato
LEFT JOIN gold.venda.fato_venda t13
    ON t1.id_contrato = t13.id_contrato_air
GROUP BY ALL
)
SELECT DISTINCT 
            codigo_contrato_air
            ,codigo_cliente
            ,flag_cnpj_sumicity
            ,data_ativacao_contrato
            ,data_ativacao_pacote_atual
            ,TRY_CAST(data_venda AS DATE) AS data_venda
            ,status_contrato
            ,tipo_pessoa
            ,segmento
            ,flag_bl
            ,flag_fx
            ,flag_tv
            ,velocidade_internet
            ,nome_campanha
            ,codigo_campanha
            ,data_vencimento_fidelidade
            ,flag_fidelizacao_atual
            ,nome_pacote
            ,codigo_pacote
            ,valor_total
            ,valor_soma_itens
            ,valor_soma_adicionais
            ,descricao_adicionais
            ,desconto_campanha
            ,TRY_CAST(data_ultimo_desconto_campanha AS DATE) AS data_ultimo_desconto_campanha
            ,desconto_retencao
            ,TRY_CAST(data_ultimo_desconto_retencao AS DATE) AS data_ultimo_desconto_retencao
            ,desconto_ajuste_comercial
            ,TRY_CAST(data_ultimo_desconto_ajuste_comercial AS DATE) AS data_ultimo_desconto_ajuste_comercial
            ,desconto_ajuste_financeiro
            ,TRY_CAST(data_ultimo_desconto_ajuste_financeiro AS DATE) AS data_ultimo_desconto_ajuste_financeiro
            ,desconto_cortesia
            ,TRY_CAST(data_ultimo_desconto_cortesia AS DATE) AS data_ultimo_desconto_cortesia
            ,CASE
                WHEN desconto_total > 100 THEN 100
                else desconto_total
            END desconto_total
            ,CASE
                WHEN desconto_total > 100 THEN ROUND(valor_total - COALESCE(valor_total * 100,0)/100,2)
                ELSE ROUND(valor_total - COALESCE(valor_total * desconto_total,0)/100,2)
            END ticket_final
            ,TRY_CAST(data_ultimo_desconto AS DATE) AS data_ultimo_desconto
            ,fabricante
            ,quantidade_portas_lan
            ,quantidade_portas_telefonia
            ,flag_wifi
            ,flag_giga
            ,tipo_wifi
            ,serial_onu
            ,modelo_onu
            ,marcador_contrato
            ,marcador_cliente
            ,data_ultimo_reajuste
            ,ticket_antes_reajuste
            ,ticket_apos_reajuste
            ,aliquota_reajuste
            ,marca
            ,cluster
            ,regional
            ,uf
            ,unidade
            ,cidade
            ,cep
            ,bairro
            ,data_referencia
            ,mes_referencia
            ,data_atualizacao
FROM cte_basao
UNION ALL
SELECT       t1.codigo_contrato_air
            ,t1.codigo_cliente
            ,t1.flag_cnpj_sumicity
            ,t1.data_ativacao_contrato
            ,t1.data_ativacao_pacote_atual
            ,TRY_CAST(t1.data_venda AS DATE) AS data_venda
            ,t1.status_contrato
            ,t1.tipo_pessoa
            ,t1.segmento
            ,t1.flag_bl
            ,t1.flag_fx
            ,t1.flag_tv
            ,t1.velocidade_internet
            ,t1.nome_campanha
            ,t1.codigo_campanha
            ,t1.data_vencimento_fidelidade
            ,t1.flag_fidelizacao_atual
            ,t1.nome_pacote
            ,t1.codigo_pacote
            ,t1.valor_total
            ,t1.valor_soma_itens
            ,t1.valor_soma_adicionais
            ,t1.descricao_adicionais
            ,t1.desconto_campanha
            ,TRY_CAST(data_ultimo_desconto_campanha AS DATE) AS data_ultimo_desconto_campanha
            ,desconto_retencao
            ,TRY_CAST(data_ultimo_desconto_retencao AS DATE) AS data_ultimo_desconto_retencao
            ,desconto_ajuste_comercial
            ,TRY_CAST(data_ultimo_desconto_ajuste_comercial AS DATE) AS data_ultimo_desconto_ajuste_comercial
            ,desconto_ajuste_financeiro
            ,TRY_CAST(data_ultimo_desconto_ajuste_financeiro AS DATE) AS data_ultimo_desconto_ajuste_financeiro
            ,desconto_cortesia
            ,TRY_CAST(data_ultimo_desconto_cortesia AS DATE) AS data_ultimo_desconto_cortesia
            ,t1.desconto_total
            ,t1.ticket_final
            ,TRY_CAST(data_ultimo_desconto AS DATE) AS data_ultimo_desconto
            ,t1.fabricante
            ,t1.quantidade_portas_lan
            ,t1.quantidade_portas_telefonia
            ,t1.flag_wifi
            ,t1.flag_giga
            ,t1.tipo_wifi
            ,t1.serial_onu
            ,t1.modelo_onu
            ,t1.marcador_contrato
            ,t1.marcador_cliente
            ,t1.data_ultimo_reajuste
            ,t1.ticket_antes_reajuste
            ,t1.ticket_apos_reajuste
            ,t1.aliquota_reajuste
            ,t1.marca
            ,t1.cluster
            ,t1.regional
            ,t1.uf
            ,t1.unidade
            ,t1.cidade
            ,t1.cep
            ,t1.bairro
            ,t1.data_referencia
            ,t1.mes_referencia
            ,t1.data_atualizacao
FROM {{this}} t1
LEFT JOIN gold.base.dim_contrato t2 ON t1.codigo_contrato_air = t2.id_contrato
WHERE t1.mes_referencia <> date_format(CURRENT_DATE()-1, 'yyyy-MM') 
      AND t1.status_contrato IN ('SUSP_DEBITO','SUSP_CANCELAMENTO','SUSP_SOLICITACAO','HABILITADO')
      AND (t2.data_cancelamento > t1.data_referencia OR t2.data_cancelamento IS NULL)  

/*
ALTER TABLE gold_dev.relatorios.tbl_basao
ADD COLUMNS (
    data_ultimo_desconto_campanha DATE,
    data_ultimo_desconto_retencao DATE,
    data_ultimo_desconto_ajuste_comercial DATE,
    data_ultimo_desconto_ajuste_financeiro DATE,
    data_ultimo_desconto_cortesia DATE
);
*/    
