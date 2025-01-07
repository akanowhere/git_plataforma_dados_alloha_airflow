-- Faturas de acordo com vencimento mes atual e passado
WITH negociadores AS (
  SELECT 
    DISTINCT 
      nm_negociador, 
      id_negociador 
    FROM gold.sydle.dim_negociacao
)
, negociacoes_count AS (
  SELECT 
    *,
    COUNT(id_negociador) q,
    ROW_NUMBER() OVER (PARTITION BY id_negociador ORDER BY nm_negociador) r,
    CASE WHEN nm_negociador LIKE '% %' THEN 1 ELSE 0 END AS has_space,
    CASE WHEN nm_negociador LIKE '%?%' THEN 1 ELSE 0 END AS has_interrogation,
    CASE WHEN nm_negociador LIKE '%ç%' THEN 1 ELSE 0 END AS has_cedilha
  FROM negociadores
  GROUP BY nm_negociador, 
      id_negociador
)
, faturas_negociacao AS (
    SELECT  
        DISTINCT
        df.data_criacao AS data_negociacao,
        df.data_vencimento,
        df.codigo_contrato_air AS contrato,
        df.codigo_fatura_sydle,
        df.valor_sem_multa_juros AS valor_fatura,
        df.valor_pago,
        df.data_pagamento,
        df.status_fatura,
        dn.id_negociacao,
        dn.id_negociador,
         COALESCE(nc.nm_negociador, dn.nm_negociador) AS negociador
    FROM gold.sydle.dim_faturas_all_versions df
    LEFT JOIN gold.sydle.dim_negociacao dn
        ON df.codigo_fatura_sydle = dn.codigo
    LEFT JOIN negociacoes_count nc
        ON dn.id_negociador = nc.id_negociador 
        AND has_interrogation = 0 
        AND has_space = 1
        AND has_cedilha = 0
    WHERE is_last = true
        AND classificacao = 'NEGOCIACAO'
        AND date(data_vencimento) >= '2024-01-01'
)
-- Email do cliente
, contato_email AS (
    SELECT
        id_cliente,
        contato AS email,
        ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS prioridade
    FROM  gold.contato.dim_contato_email
    WHERE
    tipo = 'EMAIL'
    AND excluido = 0
    AND NOT (
        contato NOT LIKE '%@%'
        OR contato LIKE '%atualiz%'
        OR contato LIKE '%sac@sumicity%'
        OR contato LIKE '%autalize%'
        OR contato LIKE '%teste%'
        OR contato LIKE '%naotem%'
        OR contato LIKE '%n_oconsta%'
        OR contato LIKE '%rca@%'
        OR contato LIKE '%sumi%'
        OR contato LIKE '%@tem%'
    )
)
-- Base analitica de negociacoes
, negociacao AS (
    SELECT  
        DISTINCT
        dn.id_negociacao,
        dn.codigo AS codigo_nova,
        dfaN.codigo_contrato_air,
        dfaN.valor_sem_multa_juros AS valor_nova,
        dn.fatura_original AS codigo_origem,
        dfaO.valor_sem_multa_juros AS valor_origem,
        dfaO.data_vencimento  AS data_vencimento_origem
    FROM gold.sydle.dim_negociacao dn
    LEFT JOIN gold.sydle.dim_faturas_all_versions dfaN
        ON dn.codigo = dfaN.codigo_fatura_sydle AND dfaN.is_last = true
    LEFT JOIN gold.sydle.dim_faturas_all_versions dfaO
        ON dn.fatura_original = dfaO.codigo_fatura_sydle AND dfaO.is_last = true
)
-- Fatuas novas de negociacao
, negociacao_nova AS (
    SELECT  
        DISTINCT
        id_negociacao,
        codigo_nova,
        valor_nova
    FROM negociacao
)
, negociacao_nova_consolidado AS (
    SELECT 
        id_negociacao, 
        ROUND(SUM(valor_nova),2) AS valor_total_divida_nova
    FROM negociacao_nova
    GROUP BY id_negociacao
)
-- Faturas origem da negociação
, negociacao_origem AS (
    SELECT  
        DISTINCT
        id_negociacao,
        codigo_origem,
        valor_origem,
        data_vencimento_origem
    FROM negociacao
)
, negociacao_origem_consolidado AS (
    SELECT 
        id_negociacao, 
        ROUND(SUM(valor_origem),2) AS valor_total_divida_origem,
        MIN(data_vencimento_origem) AS min_data_vencimento_origem
    FROM negociacao_origem
    GROUP BY id_negociacao
)
-- Base de reversao de negociacoes
, reversao AS (
    SELECT 
        DISTINCT
        du.marca,
        CAST(fn.data_negociacao AS DATE) AS data_negociacao,
        date_format(fn.data_negociacao, 'yyyy-MM-01') as mes_negociacao,
        date_format(fn.data_vencimento, 'yyyy-MM-01') as mes_vencimento,
        fn.data_vencimento,
        fn.contrato,
        fn.id_negociador,
        fn.negociador,
        usu.email  as email_negociador,
        usu.login as login_negociador,
        fn.codigo_fatura_sydle AS codigo_fatura_gerada,
        fn.valor_fatura,
        fn.valor_pago AS valor_fatura_pago,
        fn.data_pagamento,
        fn.status_fatura AS status_fatura_atual,
        date_diff(fn.data_negociacao,no.min_data_vencimento_origem) AS aging_atual,
        REPLACE(REPLACE(du.regional,'EGIAO_0',''),'EGIAO-0','') AS regional,
        no.valor_total_divida_origem,
        nn.valor_total_divida_nova,
        CONCAT(ROUND(((no.valor_total_divida_origem - nn.valor_total_divida_nova) / no.valor_total_divida_origem)*100,1),' %') AS DESCONTO_OU_ACRESCIMO_NEGOCIACAO,
        CASE
            WHEN ta.status_mailing = 'INATIVO' THEN 'CANCELADO'
            WHEN ta.status_mailing = 'ATIVO' THEN REPLACE(REPLACE(ta.faixa_aging,'[ULTIMA_CHANCE]',''),'61-90d+','61-90d')
            ELSE 'OUTROS'
        END AS mailing_inicial,
        fn.id_negociacao,
        ce.email,
        COALESCE(dcl.cpf,dcl.cnpj) AS cpf_cnpj
    FROM faturas_negociacao fn
    LEFT JOIN gold.base.dim_contrato dc
        ON fn.contrato = dc.id_contrato_air
    LEFT JOIN gold.base.dim_unidade du
        ON dc.unidade_atendimento = du.sigla AND du.excluido = false
    LEFT JOIN gold.mailing.tbl_analitico ta
        ON  date_format(fn.data_negociacao, 'yyyy-MM-01') = ta.data_referencia AND fn.contrato = ta.id_contrato
    LEFT JOIN gold.base.dim_cliente dcl
        ON dc.id_cliente = dcl.id_cliente
    LEFT JOIN contato_email ce
        ON dc.id_cliente = ce.id_cliente AND ce.prioridade = 1
    LEFT JOIN negociacao_nova_consolidado nn
        ON fn.id_negociacao = nn.id_negociacao
    LEFT JOIN negociacao_origem_consolidado no
        ON fn.id_negociacao = no.id_negociacao
    LEFT JOIN gold.sydle.dim_sydle_usuario usu
        ON  fn.id_negociador = usu.id_usuario
)
-- Basa Final
SELECT * FROM reversao;





