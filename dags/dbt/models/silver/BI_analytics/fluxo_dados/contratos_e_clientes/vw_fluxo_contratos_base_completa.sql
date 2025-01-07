-- Montando Depara Banda
WITH CTE_BANDA AS (
  SELECT DISTINCT 
    mikrotik_banda
  FROM gold.base.dim_conexao
),
CTE_VELOCIDADES AS (
  SELECT
    mikrotik_banda,
    LEFT(mikrotik_banda, CHARINDEX(' ', CONCAT(mikrotik_banda, ' ')) - 1) AS PRIMEIRA_COMBINACAO
  FROM 
    CTE_BANDA
),
CTE_CONVERTIDO AS (
  SELECT 
    mikrotik_banda,
    LEFT(PRIMEIRA_COMBINACAO, CHARINDEX('/', PRIMEIRA_COMBINACAO) - 1) AS VELOCIDADE_UP,
    RIGHT(PRIMEIRA_COMBINACAO, LEN(PRIMEIRA_COMBINACAO) - CHARINDEX('/', PRIMEIRA_COMBINACAO)) AS VELOCIDADE_DOWN
  FROM 
    CTE_VELOCIDADES
),
CTE_FINAL AS (
  SELECT 
    mikrotik_banda,
    ROUND(CASE 
      WHEN RIGHT(VELOCIDADE_UP, 1) IN ('k', 'K') THEN CAST(LEFT(VELOCIDADE_UP, LEN(VELOCIDADE_UP) - 1) AS FLOAT) / 1024
      WHEN RIGHT(VELOCIDADE_UP, 1) IN ('m', 'M') THEN CAST(LEFT(VELOCIDADE_UP, LEN(VELOCIDADE_UP) - 1) AS FLOAT)
      WHEN RIGHT(VELOCIDADE_UP, 1) IN ('g', 'G') THEN CAST(LEFT(VELOCIDADE_UP, LEN(VELOCIDADE_UP) - 1) AS FLOAT) * 1024
      ELSE VELOCIDADE_UP
    END,1) AS velocidade_up_mbps,
    ROUND(CASE 
      WHEN RIGHT(VELOCIDADE_DOWN, 1) IN ('k', 'K') THEN CAST(LEFT(VELOCIDADE_DOWN, LEN(VELOCIDADE_DOWN) - 1) AS FLOAT) / 1024
      WHEN RIGHT(VELOCIDADE_DOWN, 1) IN ('m', 'M') THEN CAST(LEFT(VELOCIDADE_DOWN, LEN(VELOCIDADE_DOWN) - 1) AS FLOAT)
      WHEN RIGHT(VELOCIDADE_DOWN, 1) IN ('g', 'G') THEN CAST(LEFT(VELOCIDADE_DOWN, LEN(VELOCIDADE_DOWN) - 1) AS FLOAT) * 1024
      ELSE VELOCIDADE_DOWN
    END,1) AS velocidade_down_mbps
  FROM 
    CTE_CONVERTIDO
),
CTE_FIDELIZADOS AS (
  SELECT
    dc.codigo_externo as CONTRATO_CODIGO_AIR, 
    1 as ind_fidelizacao_atual,
    CAST(dcd.data_fim_fidelidade AS DATE) as data_vencimento_fidelidade
  FROM gold.sydle.dim_contrato_sydle dc
  LEFT JOIN gold.sydle.dim_contrato_descontos dcd
  ON dc.id_contrato = dcd.id_contrato
  WHERE produto_nome = 'INSTALAÇÃO FIBRA'
  AND desconto_nome LIKE '%multa de fidelidade%'
  AND CAST(dcd.data_fim_fidelidade AS DATE) > CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
),
CTE_DESCONTO AS (
  SELECT id_contrato, desconto, data_validade, item_codigo, categoria, ROW_NUMBER() OVER (PARTITION BY id_contrato, categoria, item_codigo ORDER BY id DESC) AS rn
  FROM gold.base.dim_desconto t1
  WHERE data_validade >= GETDATE() AND categoria IN ('CAMPANHA', 'Ajuste comercial', 'CORTESIA', 'AJUSTE_COMERCIAL', 'RETENCAO', 'AJUSTE_FINANCEIRO')
),
CTE_DESCONTOS_ATIVOS AS (
  SELECT id_contrato,
         item_codigo,
         SUM(desconto) as desconto_total,
         MAX(data_validade) max_data_validade 
  FROM CTE_DESCONTO t1
  WHERE t1.rn = 1
  GROUP BY id_contrato, item_codigo
),
CTE_CLIENTES_PROPRIOS AS (
  SELECT id_cliente
  FROM gold.base.dim_cliente cli
  WHERE (TRIM(cli.nome) LIKE '%SUMICITY%' 
        OR TRIM(cli.nome) LIKE '%VM OPENLINK%' 
        OR TRIM(cli.nome) LIKE '%VELOMAX%') AND cli.fonte = 'AIR'
),
CTE_EMAIL AS (
  SELECT
    id_cliente,    
    contato AS EMAIL,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) prioridade
  FROM gold.contato.dim_contato_email tcontato
  WHERE 
    tcontato.tipo = 'EMAIL' 
    AND tcontato.excluido = '0' AND
    NOT(
      tcontato.contato NOT LIKE '%@%' 
      OR tcontato.contato LIKE '%atualiz%' 
      OR tcontato.contato LIKE '%sac@sumicity%' 
      OR tcontato.contato LIKE '%autalize%' 
      OR tcontato.contato LIKE '%teste%' 
      OR tcontato.contato LIKE '%naotem%' 
      OR tcontato.contato LIKE '%n_oconsta%' 
      OR tcontato.contato LIKE '%rca@%' 
      OR tcontato.contato LIKE '%sumi%' 
      OR tcontato.contato LIKE '%@tem%')
),
CTE_EMAIL_FINAL AS (
  SELECT 
    id_cliente,
    MAX(CASE
      WHEN prioridade = 1 THEN EMAIL
      ELSE NULL
    END) EMAIL,
    CONCAT_WS(',', collect_list(EMAIL)) AS TODOS_EMAILS      
  FROM CTE_EMAIL
  GROUP BY id_cliente  
),
CTE_WHATSAPP AS (
  SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS WHATSAPP
  FROM gold.contato.dim_contato_telefone
  WHERE fonte_contato LIKE 'sumicity%'
),
CTE_SMS AS (
  SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS telefone,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) r_num
  FROM gold.contato.dim_contato_telefone
  WHERE 
    excluido = '0'
    AND classificacao_contato IN ('MOVEL')
    AND existe_air = 'S'
    AND NOT fonte_contato LIKE 'sumicity%'
),
CTE_SMS_FINAL AS (
  SELECT 
    id_cliente,
    telefone
  FROM CTE_SMS
  WHERE r_num = 1
),
CTE_CONTATO AS (
  SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS telefone,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) prioridade
  FROM gold.contato.dim_contato_telefone
  WHERE 
    excluido = '0'
    AND classificacao_contato IN ('MOVEL', 'FIXO')
    AND existe_air = 'S'
    AND NOT fonte_contato LIKE 'sumicity%'
),
CTE_CONTATO_FINAL AS (
  SELECT 
    dct.id_cliente,
    MAX(TEL_01) AS tel_01,
    MAX(TEL_02) AS tel_02,
    MAX(TEL_03) AS tel_03,
    MAX(WHATSAPP) AS whatsapp,
    MAX(s.telefone) AS sms,
    MAX(EMAIL) AS email,
    MAX(TODOS_EMAILS) as todos_emails,
    DATEADD(HOUR, -3, GETDATE()) AS data_extracao
  FROM gold.contato.dim_contato_telefone dct
  LEFT JOIN (
    SELECT 
      id_cliente,
      telefone,
      CASE
        WHEN prioridade = 1 THEN telefone
        ELSE NULL
      END TEL_01,
      CASE
        WHEN prioridade = 2 THEN telefone
        ELSE NULL
      END TEL_02,
      CASE
        WHEN prioridade = 3 THEN telefone
        ELSE NULL
      END TEL_03       
    FROM CTE_CONTATO
  ) AS PivotData
    ON dct.id_cliente = PivotData.id_cliente
  LEFT JOIN CTE_WHATSAPP ws
    ON dct.id_cliente = ws.id_cliente
  LEFT JOIN CTE_EMAIL_FINAL e
    ON dct.id_cliente = e.id_cliente
  LEFT JOIN CTE_SMS_FINAL s
    ON dct.id_cliente = s.id_cliente
  GROUP BY dct.id_cliente
)

-- Seleciona informações detalhadas dos contratos dos clientes
SELECT t1.id_contrato AS contrato_air,
       t1.id_cliente AS codigo_cliente,
       CASE
         WHEN t8.id_cliente IS NOT NULL THEN 'SIM'
         ELSE 'NAO'
       END expurgo_cnpj_proprio,
       t2.nome AS nome_cliente,
       COALESCE(t2.cpf, t2.cnpj) AS cpf_cnpj,
       t2.tipo AS tipo_pessoa,
       CASE
         WHEN t1.b2b = TRUE THEN 'B2B'
         WHEN t1.pme = TRUE THEN 'PME'
         ELSE 'B2C'
       END segmento,
       t1.data_primeira_ativacao AS data_ativacao,
       t1.data_cancelamento AS data_cancelamento,
       t1.pacote_codigo AS codigo_pacote,
       t1.pacote_nome AS nome_pacote,
       REPLACE(t1.status, 'ST_CONT_', '') AS status_contrato,
       t1.unidade_atendimento,
       t1.valor_final AS valor_final,
       t1.VALOR_PADRAO_PLANO AS valor_soma_itens,
       t1.VALOR_ADICIONAIS AS valor_soma_itens_adicionais,
       t1.ADICIONAL_DESC AS nome_adicionais,
       t11.desconto_total,
       DATEDIFF(DAY, t11.max_data_validade, GETDATE()) as tempo_desconto,
       t2.cupom,
       t1.marcador AS marcador_contrato,
       t2.marcador AS marcador_principal_cliente,
       t12.marcador_secundario_cliente,
       t7.marca,
       t7.regional,
       t7.cluster,
       t3.id_endereco,
       t4.estado,
       t4.cidade,
       t4.bairro,
       t4.cep,
       t4.logradouro,
       t4.referencia,
       t4.numero,
       t4.complemento,
       t4.longitude,
       t4.latitude,
       t5.onu_modelo,
       t5.onu_serial,
       t5.descricao_banda AS velocidade,
       t5.mikrotik_banda,
       t5.velocidade_down_mbps,
       t5.velocidade_up_mbps,
       t6.tel_01,
       t6.tel_02,
       t6.tel_03,
       t6.whatsapp,
       t6.sms,
       t6.email,
       t6.todos_emails,
       t9.id_contrato_reajuste,
       t9.data_processamento data_ultimo_reajuste,
       t9.valor_anterior AS ticket_antes_reajuste,
       t9.valor_final AS ticket_apos_reajuste,
       t9.valor_indice AS aliquota_reajuste,
       t10.data_vencimento_fidelidade,
       t10.ind_fidelizacao_atual,
       GETDATE() as data_atualizacao
FROM gold.base.dim_contrato t1
LEFT JOIN gold.base.dim_cliente t2 ON (t1.id_cliente = t2.id_cliente AND t2.fonte = 'AIR' AND t2.excluido = 'false')
LEFT JOIN (
  SELECT id_contrato, MAX(id_endereco) AS id_endereco
  FROM gold.base.dim_contrato_entrega
  WHERE excluido = FALSE
  GROUP BY id_contrato
) t3 ON (t1.id_contrato = t3.id_contrato)
LEFT JOIN gold.base.dim_endereco t4 ON (t3.id_endereco = t4.id_endereco AND t4.fonte = 'AIR')
LEFT JOIN (
  SELECT id_login,
         contrato_codigo,
         onu_serial,
         onu_modelo,
         descricao_banda,
         t1.mikrotik_banda,
         ROW_NUMBER() OVER (PARTITION BY contrato_codigo ORDER BY id_login DESC) AS rn,
         t2.velocidade_up_mbps,
         t2.velocidade_down_mbps
  FROM gold.base.dim_conexao t1
  LEFT JOIN CTE_FINAL t2 ON (t1.mikrotik_banda = t2.mikrotik_banda)
) t5 ON (t1.id_contrato = t5.contrato_codigo AND t5.rn = 1)
LEFT JOIN CTE_CONTATO_FINAL t6 ON (t1.id_cliente = t6.id_cliente)
LEFT JOIN gold.base.dim_unidade t7 ON (t1.unidade_atendimento = t7.sigla AND t7.fonte = 'AIR')
LEFT JOIN CTE_CLIENTES_PROPRIOS t8 ON (t1.id_cliente = t8.id_cliente)
LEFT JOIN (
  SELECT id_contrato_reajuste,
         id_contrato,
         data_processamento,
         valor_anterior,
         porcentagem_reajuste AS valor_indice,
         valor_final,
         ROW_NUMBER() OVER (PARTITION BY id_contrato ORDER BY data_processamento DESC) AS rn
  FROM gold.base.dim_contrato_reajuste
) t9 ON (t1.id_contrato = t9.id_contrato AND t9.rn = 1)
LEFT JOIN CTE_FIDELIZADOS t10 ON (t1.id_contrato = t10.CONTRATO_CODIGO_AIR)
LEFT JOIN CTE_DESCONTOS_ATIVOS t11 ON (t1.id_contrato = t11.id_contrato AND t1.pacote_codigo = t11.item_codigo)
LEFT JOIN (
  SELECT
    id_cliente,
    CONCAT_WS(', ', COLLECT_LIST(marcador)) AS marcador_secundario_cliente
  FROM gold.base.dim_outros_marcadores
  GROUP BY id_cliente
) t12 ON t1.id_cliente = t12.id_cliente;