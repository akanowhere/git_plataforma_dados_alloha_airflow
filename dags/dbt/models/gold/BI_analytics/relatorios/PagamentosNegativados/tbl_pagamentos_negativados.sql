-- Query de teste

{% set dates = get_dates(['today', 'yesterday']) %}

WITH cte_fila AS (
  SELECT 
    id_fila,
    nome as nome_fila
  FROM {{ get_catalogo('gold') }}.chamados.dim_fila
  WHERE nome  LIKE ANY ('%Jurídico%','%Ouvidoria%','%FINANCEIRO - CONTESTAÇÃO DE FATURA%','%FINANCEIRO - OUVIDORIA/JURIDICO%','%ANÁLISE DUPLICATA%')
)
, cte_chamados AS (
  SELECT  
    t1.data_abertura,
    t2.nome_fila,
    t1.codigo_cliente,
    ROW_NUMBER() OVER (PARTITION BY t1.codigo_cliente ORDER BY data_abertura DESC) AS rn
  FROM {{ get_catalogo('gold') }}.chamados.dim_chamado t1
  INNER JOIN cte_fila t2 ON t1.id_fila = t2.id_fila
)
, cte_faturas AS (
  SELECT  
    codigo_fatura_sydle,
    codigo_contrato_air AS contrato_air,
    codigo_cliente_air AS id_cliente_air,
    data_pagamento,
    data_vencimento,
    valor_pago,
    data_atualizacao,
    valor_sem_multa_juros,
    status_fatura
  FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_all_versions
  WHERE is_last = TRUE AND (data_pagamento >= DATEADD(MONTH, -3, '{{ dates.today }}') AND status_fatura = 'Paga') OR 
        (status_fatura IN ('EMITIDA','AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO','AGUARDANDO BAIXA EM SISTEMA EXTERNO')) OR
        (status_fatura IN ('ABONADA','CANCELADA') AND data_atualizacao >= DATEADD(MONTH, -3, '{{ dates.today }}'))
)
, cte_negociacoes AS (
  SELECT 
    codigo,
    COUNT(DISTINCT fatura_original) AS qtd_faturas_negociadas,
    CONCAT_WS(', ', COLLECT_LIST(fatura_original)) AS faturas_negociadas
  FROM (
      SELECT DISTINCT codigo, fatura_original
      FROM {{ get_catalogo('gold') }}.sydle.dim_negociacao
  ) t1
  GROUP BY codigo
)
SELECT DISTINCT
  CASE 
    WHEN t2.tipo = 'FISICO' THEN CONCAT('F', regexp_replace(COALESCE(t2.cpf,t2.cnpj), '[^0-9]', ''))
    WHEN t2.tipo = 'JURIDICO' THEN CONCAT('J', regexp_replace(COALESCE(t2.cpf,t2.cnpj), '[^0-9]', ''))
  END documento_do_devedor,
  t4.codigo_fatura_sydle AS codigo_fatura,
  t4.data_vencimento AS data_vencimento,
  t4.valor_sem_multa_juros as valor_em_aberto,
  t4.data_pagamento AS data_compromisso,  
  t4.valor_pago AS valor_compromisso,
  t1.id_contrato AS codigo_contrato_air,
  t2.nome AS nome_principal, 
  t3.logradouro AS endereco,
  t3.complemento AS complemento_endereco,
  t3.bairro,
  t3.cidade,
  t3.estado,
  t3.cep, 
  t2.marcador,
  t4.status_fatura,
  t1.id_cliente AS codigo_cliente,
  t6.nome_fila AS fila_recente,
  t6.data_abertura AS data_abertura_fila,
  t4.data_atualizacao AS data_ultima_alteracao_fatura, 
  t5.qtd_faturas_negociadas AS quantidade_faturas_negociadas,
  t5.faturas_negociadas,
  '{{ dates.yesterday }}' as data_referencia
FROM {{ get_catalogo('gold') }}.base.dim_contrato t1
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t2 
  ON (t1.id_cliente = t2.id_cliente and t2.fonte = 'AIR')
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t3 
  ON (t1.id_endereco_cobranca = t3.id_endereco and t3.fonte = 'AIR')
INNER JOIN cte_faturas t4 
  ON (t1.id_contrato_air = t4.contrato_air)
LEFT JOIN cte_negociacoes t5 
  ON (t4.codigo_fatura_sydle = t5.codigo)
LEFT JOIn cte_chamados t6 
  ON (t1.id_cliente = t6.codigo_cliente and t6.rn = 1)
WHERE t2.marcador = 'MRC_NEGATIVADO' and t1.data_cancelamento IS NOT NULL;








