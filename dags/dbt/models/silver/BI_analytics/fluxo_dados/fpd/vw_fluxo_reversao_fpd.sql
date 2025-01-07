WITH cte_email AS (
  SELECT
    id_cliente,    
    contato AS EMAIL,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS rn
  FROM  {{ get_catalogo('gold') }}.contato.dim_contato_email
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
, cte_whatsapp AS (
  SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS WHATSAPP,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS rn
  FROM  {{ get_catalogo('gold') }}.contato.dim_contato_telefone
  WHERE fonte_contato LIKE 'sumicity%' AND autorizado = true AND excluido = true
)
, cte_contatos AS (
  SELECT
    id_cliente,
    CONCAT('+55', telefone_tratado) AS telefone,
    ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) AS rn
  FROM  {{ get_catalogo('gold') }}.contato.dim_contato_telefone
  WHERE 
    excluido = 0
    AND classificacao_contato IN ('MOVEL', 'FIXO')
    AND existe_air = 'S'
    AND NOT fonte_contato LIKE 'sumicity%'
)
, cte_faturas AS (
    SELECT  contrato_air,
            codigo_fatura_sydle,
            classificacao,
            status_fatura,
            valor_fatura,
            data_vencimento,
            ROW_NUMBER()OVER(PARTITION BY contrato_air ORDER BY data_vencimento ASC) rn
    FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing
    WHERE status_fatura not in ('Cancelada','Abonada')
)
SELECT  
  t1.id_contrato AS codigo_contrato_air,
  t1.id_cliente AS codigo_cliente,
  t1.data_primeira_ativacao AS data_ativacao,
  t1.data_cancelamento AS data_cancelamento,
  t1.status AS status_contrato,
  t1.unidade_atendimento,
  t5.marca AS marca,
  t2.nome AS nome_cliente,
  COALESCE(t2.cpf, t2.cnpj) AS cpf_cnpj,
  t7.EMAIL AS email,
  t8.WHATSAPP AS whatsapp,
  t6.telefone ultimo_telefone,
  t3.codigo_fatura_sydle,
  t3.classificacao,
  t3.status_fatura,
  CASE WHEN t3.rn = 1 
    THEN 'FPD'
  WHEN t3.rn = 2 
    THEN 'SPD'            
  END AS tipo_fatura,
  t3.valor_fatura,
  t3.data_vencimento,
  CASE WHEN t3.data_vencimento < current_date() 
    THEN DATEDIFF(DAY,t3.data_vencimento,current_date())
    ELSE NULL
  END aging_atraso,
  t1.legado_sistema AS sistema_origem,
  t5.regional AS regional,
  t4.canal_tratado AS canal,
  t4.equipe,
  date_sub(current_date(), 1) AS data_referencia
FROM {{ get_catalogo('gold') }}.base.dim_contrato t1
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t2 
  ON (t1.id_cliente = t2.id_cliente AND t2.fonte = 'AIR')
LEFT JOIN cte_faturas t3 
  ON (t1.id_contrato = t3.contrato_air AND t3.rn in (1,2))
LEFT JOIN {{ get_catalogo('gold') }}.venda.fato_venda t4 
  ON (t1.id_contrato = t4.id_contrato_air)
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade t5 
  ON (t1.unidade_atendimento = t5.sigla AND t5.excluido = false AND t5.fonte = 'AIR')
LEFT JOIN cte_contatos t6 
  ON (t1.id_cliente = t6.id_cliente AND t6.rn = 1)
LEFT JOIN cte_email t7 
  ON (t1.id_cliente = t7.id_cliente AND t7.rn = 1)
LEFT JOIN cte_whatsapp t8 
  ON (t1.id_cliente = t8.id_cliente AND t8.rn = 1)
  WHERE t1.data_primeira_ativacao >= '2023-01-01'










