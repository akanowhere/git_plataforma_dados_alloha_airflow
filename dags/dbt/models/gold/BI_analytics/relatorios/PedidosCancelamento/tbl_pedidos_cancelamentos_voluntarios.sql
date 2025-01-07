WITH pedidos_filtrados AS (
    SELECT 
        pr.id,
        pr.id_contrato,
        TO_TIMESTAMP(pr.data_criacao) AS data_criacao,
        pr.servico,
        pr.primeiro_nivel,
        pr.segundo_nivel,
        pr.motivo,
        pr.submotivo,
        pr.tipo_cliente,
        pr.usuario_criacao,
        pr.origem_contato,
        ROW_NUMBER() OVER (
            PARTITION BY pr.id_contrato, CAST(pr.data_criacao AS DATE), pr.usuario_criacao, pr.motivo, pr.primeiro_nivel
            ORDER BY pr.data_criacao DESC
        ) AS rn
    FROM 
        silver.stage.vw_air_tbl_perfilacao_retencao pr
    WHERE 
        pr.servico = 'retencao' 
        AND pr.primeiro_nivel = 'COM_RET_PN_RET' 
        AND pr.tipo_cliente NOT LIKE '%INAD%'
)
-- Seleciona apenas os pedidos mais recentes (rn = 1)
,retidos AS (
SELECT 
    pf.id,
    pf.data_criacao AS data,
    dc.data_primeira_ativacao AS data_ativacao,
    pf.id_contrato AS codigo_contrato,
    COALESCE(dc1.nome,motivo) AS motivo,
    COALESCE(dc2.nome,submotivo) AS submotivo,
    COALESCE(dc3.nome,primeiro_nivel) AS primeiro_nivel,
    COALESCE(dc4.nome,segundo_nivel) AS segundo_nivel,
    pf.usuario_criacao AS usuario,
    dc.unidade_atendimento AS unidade,
    pf.origem_contato,
    du.nome as nome_unidade,
    du.marca,
    REPLACE(UPPER(TRANSLATE(de.cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")),"'"," ") AS cidade,
    de.estado,
    'SOLICITACAO' AS tipo

FROM pedidos_filtrados pf
INNER JOIN gold.base.dim_contrato dc ON pf.id_contrato = dc.id_contrato_air AND dc.b2b = false
LEFT JOIN gold.base.dim_unidade du ON dc.unidade_atendimento = du.sigla AND du.excluido = false
LEFT JOIN gold.base.dim_catalogo dc1 ON pf.motivo = dc1.codigo
LEFT JOIN gold.base.dim_catalogo dc2 ON pf.submotivo = dc2.codigo
LEFT JOIN gold.base.dim_catalogo dc3 ON pf.primeiro_nivel = dc3.codigo
LEFT JOIN gold.base.dim_catalogo dc4 ON pf.segundo_nivel = dc4.codigo
LEFT JOIN gold.base.dim_endereco de ON dc.id_endereco_cobranca = de.id_endereco
WHERE 
    rn = 1 AND pf.data_criacao >=trunc(add_months(CURRENT_DATE(), -12), 'MM'))
,cancelados AS (
    SELECT  fc.id_contrato AS id,
            CASE
              WHEN CAST(fc.data_cancelamento AS DATE) = CAST(cr.data_criacao AS DATE)  THEN TO_TIMESTAMP(cr.data_criacao)
              ELSE TO_TIMESTAMP(fc.data_cancelamento)
            END AS data,
            dc.data_primeira_ativacao AS data_ativacao,
            fc.id_contrato AS codigo_contrato,
            REPLACE(REPLACE(REPLACE(REPLACE(CASE
                WHEN fc.data_cancelamento < '2024-07-01' THEN  COALESCE(dc1.nome,dc.cancelamento_motivo)
                ELSE COALESCE(dc2.nome,cr.motivo_solicitacao)
            END,'(Retenção)', ''),'(Comercial)',''),'(Ouvidoria)',''),'(Cobrança)','') AS motivo,
            COALESCE(dc3.nome,cr.sub_motivo_cancelamento) AS submotivo,   
            'CANCELADO' AS primeiro_nivel,
            CASE
                WHEN fc.cancelamento_invol ='MANUAL' and fc.tipo_cancelamento = 'INVOLUNTARIO'  THEN 'CANCELAMENTO_INVOL_ADICIONAL'
                ELSE 'CANCELAMENTO_VOL'
            END As segundo_nivel,
            cr.usuario_criacao AS usuario,
            fc.unidade,
            cr.origem_contato,
            du.nome as nome_unidade,
            du.marca,
            REPLACE(UPPER(TRANSLATE(fc.cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")),"'"," ")  AS cidade,
            fc.estado,
            'CANCELAMENTO' AS tipo

    FROM gold.base.fato_cancelamento fc
    LEFT JOIN gold.base.dim_contrato dc ON fc.id_contrato = dc.id_contrato_air AND dc.b2b = false
    LEFT JOIN gold.base.dim_contrato_retencao cr ON fc.id_contrato = cr.id_contrato AND cr.status = 'RT_CANCELADO'
    LEFT JOIN gold.base.dim_unidade du ON fc.unidade = du.sigla AND du.excluido = false
    LEFT JOIN gold.base.dim_catalogo dc1 ON UPPER(dc.cancelamento_motivo) = UPPER(dc1.codigo) AND dc1.excluido = 0
    LEFT JOIN gold.base.dim_catalogo dc2 ON UPPER(cr.motivo_solicitacao) = UPPER(dc2.codigo) AND dc2.excluido = 0
    LEFT JOIN gold.base.dim_catalogo dc3 ON cr.sub_motivo_cancelamento = dc3.codigo AND dc3.excluido = 0    
    WHERE fc.fonte = 'AIR' AND fc.segmento <> 'B2B' AND fc.data_cancelamento  >= trunc(add_months(CURRENT_DATE(), -12), 'MM') AND (fc.tipo_cancelamento = 'VOLUNTARIO' OR (fc.tipo_cancelamento = 'INVOLUNTARIO'  AND fc.cancelamento_invol = 'MANUAL'))
)
,descontos_ret AS (
    SELECT  dd.id,
            TO_TIMESTAMP(dd.data_criacao) AS data,
            dc.data_primeira_ativacao AS data_ativacao,
            dd.id_contrato AS codigo_contrato,
            'Sem Perfilação' AS motivo,
            'Sem Perfilação' AS submotivo,
            'Retido' AS primeiro_nivel,
            CAST(dd.desconto AS STRING) AS segundo_nivel,
            dd.usuario_criacao AS usuario,
            dc.unidade_atendimento AS unidade,
            NULL AS origem_contato,
            du.nome as nome_unidade,
            du.marca,
            REPLACE(UPPER(TRANSLATE(de.cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")),"'"," ")  AS cidade,
            de.estado,
            'SEM_PERFILACAO' AS tipo
    FROM gold.base.dim_desconto dd
    INNER JOIN gold.base.dim_contrato dc ON dd.id_contrato = dc.id_contrato_air AND dc.b2b = false 
    LEFT JOIN retidos r ON dd.id_contrato = r.codigo_contrato AND CAST(dd.data_criacao AS DATE) = CAST(r.data AS DATE)  AND dd.usuario_criacao = r.usuario
    LEFT JOIN gold.base.dim_unidade du ON dc.unidade_atendimento = du.sigla AND du.excluido = false
    LEFT JOIN gold.base.dim_endereco de ON dc.id_endereco_cobranca = de.id_endereco
    WHERE dd.categoria = 'RETENCAO' AND r.id IS NULL AND dd.data_criacao >= trunc(add_months(CURRENT_DATE(), -12), 'MM') AND dd.excluido = false AND dd.desconto >0
)
,cte AS (
    SELECT  r.*,du.email, COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional, COALESCE(sr.sigla_regional,'Sem Classificacao') AS regional, COALESCE(sr.subregional,'Sem Classificacao') AS subregional
    FROM retidos r
    LEFT JOIN silver.stage_seeds_data.subregionais sr ON r.cidade = UPPER(sr.cidade_sem_acento) AND r.estado = sr.uf
    LEFT JOIN gold.base.dim_usuario du ON r.usuario = du.codigo
    UNION ALL
    SELECT c.*,du.email, COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional, COALESCE(sr.sigla_regional,'Sem Classificacao') AS regional, COALESCE(sr.subregional,'Sem Classificacao') AS subregional
    FROM cancelados c
    LEFT JOIN silver.stage_seeds_data.subregionais sr ON c.cidade = UPPER(sr.cidade_sem_acento) AND c.estado = sr.uf
    LEFT JOIN gold.base.dim_usuario du ON c.usuario = du.codigo
    UNION ALL
    SELECT d.*,du.email, COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional, COALESCE(sr.sigla_regional,'Sem Classificacao') AS regional, COALESCE(sr.subregional,'Sem Classificacao') AS subregional
    FROM descontos_ret d
    LEFT JOIN silver.stage_seeds_data.subregionais sr ON d.cidade = UPPER(sr.cidade_sem_acento) AND d.estado = sr.uf
    LEFT JOIN gold.base.dim_usuario du ON d.usuario = du.codigo
)
,cte_reicindencia AS (
    SELECT t1.*,COUNT(DISTINCT t2.id) as qtd_pedidos_90d, MAX(t2.id) AS ultimo_id_90d
FROM cte t1
LEFT JOIN cte t2 ON t1.codigo_contrato = t2.codigo_contrato AND  t2.data < t1.data  AND t2.data > DATE_SUB(t1.data,90) AND t2.tipo = 'SOLICITACAO'
GROUP BY t1.id, t1.data, t1.data_ativacao, t1.codigo_contrato, t1.motivo, t1.submotivo, t1.primeiro_nivel, t1.segundo_nivel, 
         t1.usuario, t1.unidade, t1.origem_contato, t1.nome_unidade, t1.marca, t1.cidade, t1.estado, t1.tipo, t1.email, t1.macro_regional, t1.regional, t1.subregional
)
SELECT t1.*,
       t2.data AS data_ultima_ret,
       t2.segundo_nivel AS ferramenta_ultima_ret,
       t2.motivo AS motivo_ultima_ret,       
        CASE
            WHEN UPPER(TRIM(t1.motivo)) IN (
                'ÁREA DE RISCO', 'CANCELADO ANTES DA HABILITAÇÃO', 'CANCELADO ANTES DE 7 DIAS', 'CM_CLIENTE_CANCEL_RCA', 
                'CM_JURIDICO', 'CM_NOVO_PLANO', 'CM_QUALIFY', 'CONTRATO TEMPORÁRIO PARA EVENTO', 'LIGAÇÃO CAIU', 
                'N2: ANORMALIDADE', 'N2: EQUIPAMENTO PESSOAL', 'N2: RESOLVIDO', 'N2: SEM CONTATO', 'NA', 'NOVO CONTRATO', 
                'ÓBITO DO TITULAR', 'RESCISÃO', 'RETIDO', 'OSTARA', 'N2: INDEVIDO SOLUCIONADO', 'SERVICE DESK TV', 
                'N2: INDEVIDO TRANSFERIDO', 'SEM PERFILAÇÃO', 'SEM PERFILAÃƒÂ§ÃƒÂ£O', 'FALHA ADAPTER', 'CORTESIA', 
                'FALHA OPERACIONAL', 'MUDANÇA DE PLANO', 'TROCA DE TITULARIDADE', 'MIGRAÇÃO DE TECNOLOGIA', 
                'SUSPEITA DE FRAUDE', 'LNI - NÃO CONTACTADO', 'LNI - VENDA DUPLICADA'
            ) THEN 'Outros'
            WHEN UPPER(TRIM(t1.motivo)) IN (
                'DESEJA PLANO PROMOCIONAL', 'FINANCEIRO', 'MUDANÇA DE PROVEDOR DEVIDO AO PREÇO', 'MUDANÇA PROVEDOR', 
                'NEGOCIAÇÃO REALIZADA', 'PLANO PROMOCIONAL', 'PROBLEMAS FINANCEIROS', 'REAJUSTE ICMS', 
                'MUDANÇA DE PROVEDOR - PREÇO', 'PROBLEMAS FATURAMENTO', 'CONCORRÊNCIA', 'COM_RET_MTV_CM_FINANCEIRO'
            ) THEN 'Motivos Financeiros'
            WHEN UPPER(TRIM(t1.motivo)) IN (
                'INSATISFAÇÃO COM ATENDIMENTO', 'INSATISFAÇÃO COM O ATENDIMENTO', 'INSATISFAÇÃO COM O SERVIÇO', 
                'INSATISFAÇÃO COM SERVIÇO', 'INSATISFAÇÃO PELO ATENDIMENTO', 'MUDANÇA DE PROVEDOR DEVIDO A QUALIDADE', 
                'REPARO', 'MUDANÇA DE PROVEDOR POR QUALIDADE', 'INSATISFAÇÃO - VISITA TÉCNICA', 'INSATISFAÇÃO POR CONEXÃO', 
                'MUDANÇA DE PROVEDOR - QUALIDADE', 'INSATISFAÇÃO PELO SERVIÇO', 'INSATISFAÇÃO POR LENTIDÃO', 
                'INSATISFAÇÃO POR ATENDIMENTO CALL CENTER'
            ) THEN 'Qualidade Serviço'
            WHEN UPPER(TRIM(t1.motivo)) IN (
                'MUD. ENDEREÇO INVIABILIDADE', 'MUDANÇA DE ENDEREÇO', 'MUD. ENDEREÇO VIABILIDADE/ PARCIAL', 
                'MUD. ENDEREÇO VIABILIDADE/PARCIAL', 'MUDANCA DE ENDEREÇO'
            ) THEN 'Mudança de Endereço'
            ELSE 'Outros'
        END AS motivo_grupo,
    CASE
        WHEN UPPER(TRIM(t1.segundo_nivel)) IN (
            'ANORMALIDADE', 'CLIENTE DESISTIU/LIGA DEPOIS', 'FINANCEIRO', 'INFORMAÇÃO DE OFERTA', 
            'ISENÇÃO - CONTROLE REMOTO', 'ISENÇÃO - METRAGEM EXCEDIDA', 'ISENÇÃO - MUDANÇA DE CÔMODO', 
            'NA', 'ORDEM DE SERVIÇO', 'SEM PERFILAÇÃO', 'SEM PERFILAÃƒÂ§ÃƒÂ£O', 'TROCA DE TITULARIDADE'
        ) THEN 'Outro'
        WHEN UPPER(TRIM(t1.segundo_nivel)) IN (
            'ARGUMENTAÇÃO', 'CUSTO BENEFÍCIO DO PLANO ATUAL', 'FIDELIDADE/MULTA', 'MUDANÇA DE ENDEREÇO', 
            'OUTROS ARGUMENTAÇÃO', 'PONTOS FORTES DA MARCA'
        ) THEN 'Argumentação'
        WHEN UPPER(TRIM(t1.primeiro_nivel)) IN ('CANCELADO') THEN 'Cancelado'
        WHEN UPPER(TRIM(t1.segundo_nivel)) IN (
            'DESCONTO 10%', 'DESCONTO 15%', 'DESCONTO 20%', 'DESCONTO 25%', 'DESCONTO 30%', 
            'DESCONTO 35%', 'DESCONTO 40%', 'DESCONTO 45%', 'DESCONTO 5%', 'DESCONTO 50%'
        ) THEN 'Descontos'
        WHEN UPPER(TRIM(t1.segundo_nivel)) IN (
            'DOWNGRADE', 'TROCA DE PLANO', 'UPGRADE'
        ) THEN 'Troca de Plano'
        WHEN UPPER(TRIM(t1.segundo_nivel)) IN (
            'PRIORIDADE EM O.S.', 'REPARO'
        ) THEN 'Prioridade em O.S. & Reparo'
        WHEN UPPER(TRIM(t1.segundo_nivel)) = 'TROCA DE PLANO & DESCONTO' THEN 'Troca de Plano & Desconto'
        ELSE 'Outro'
    END AS segundo_nivel_grupo,
    current_date()-1 AS data_referencia

FROM cte_reicindencia t1
LEFT JOIN cte t2 ON (t1.codigo_contrato = t2.codigo_contrato and t1.ultimo_id_90d = t2.id)
LEFT JOIN gold.base.dim_contrato t3 ON t1.codigo_contrato = t3.id_contrato