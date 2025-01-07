WITH temp_descontos AS (
  SELECT t1.id_contrato,
         t1.id AS id_desconto,
         t1.data_criacao,
         t1.data_validade,
         REPLACE(UPPER(t1.categoria),'_',' ') AS categoria,
         t1.desconto,
         t1.item_codigo,
         CASE
           WHEN t3.data_venda IS NOT NULL AND t3.data_venda >= t1.dia_aplicar AND t3.data_venda <= t1.data_validade THEN t3.data_venda
           WHEN t1.excluido = true AND t1.data_alteracao < t1.data_validade THEN t1.data_alteracao
           ELSE t1.data_validade
         END AS data_final_desconto,
         CASE
           WHEN t1.item_codigo = t4.pacote_codigo AND t1.data_validade > CURRENT_DATE() - 1 AND t1.excluido = false THEN 1
           ELSE 0
         END AS desconto_ativo
  FROM gold.base.dim_desconto t1
  INNER JOIN (
    SELECT categoria, id_contrato, MAX(id) AS ultimo_id_desconto
    FROM gold.base.dim_desconto
    GROUP BY categoria, id_contrato
  ) t2 ON (t1.id_contrato = t2.id_contrato AND t1.categoria = t2.categoria AND t1.id = t2.ultimo_id_desconto)
  LEFT JOIN gold.base.dim_migracao t3 ON (t1.id_contrato = t3.id_contrato AND t1.item_codigo = t3.antigo_pacote_codigo AND t3.reprovada = false)
  LEFT JOIN gold.base.dim_contrato t4 ON (t1.id_contrato = t4.id_contrato)
  WHERE t4.data_primeira_ativacao IS NOT NULL
    AND t4.data_cancelamento IS NULL
    AND t1.categoria IN ('Ajuste comercial', 'AJUSTE_COMERCIAL', 'AJUSTE_FINANCEIRO', 'CAMPANHA', 'CORTESIA', 'RETENCAO')
    AND desconto > 0
    AND t1.data_validade >= DATEADD(MONTH, -14, CURRENT_DATE())
),
temp_desconto_consolidado AS (
  SELECT id_contrato,
         desconto_ativo,
         CASE
           WHEN categoria <> 'CAMPANHA' THEN 'OUTROS'
           WHEN categoria = 'CAMPANHA' THEN 'CAMPANHA'
           ELSE '-'
         END AS categoria,
         SUM(desconto) AS desconto,
         MAX(data_final_desconto) AS data_final_desconto
  FROM temp_descontos
  WHERE (data_final_desconto >= DATEADD(MONTH, -14, CURRENT_DATE()) AND categoria = 'CAMPANHA')
     OR (data_final_desconto >= DATEADD(MONTH, -6, CURRENT_DATE()) AND categoria <> 'CAMPANHA')
  GROUP BY id_contrato, desconto_ativo, 
           CASE
             WHEN categoria <> 'CAMPANHA' THEN 'OUTROS'
             WHEN categoria = 'CAMPANHA' THEN 'CAMPANHA'
             ELSE '-'
           END
)
,temp_desconto_categoria AS (
  SELECT id_contrato,
         desconto_ativo,
         categoria,
         SUM(desconto) AS desconto,
         MAX(data_final_desconto) AS data_final_desconto
  FROM temp_descontos
  WHERE (data_final_desconto >= DATEADD(MONTH, -14, CURRENT_DATE()) AND categoria = 'CAMPANHA')
     OR (data_final_desconto >= DATEADD(MONTH, -6, CURRENT_DATE()) AND categoria <> 'CAMPANHA')
  GROUP BY id_contrato, desconto_ativo, categoria
           
)
SELECT CONTRATO AS codigo_contrato_air,
       STATUS_CONTRATO AS status_contrato,
       MARCA AS marca,
      CODIGO_PACOTE AS codigo_pacote,
       NOME_PACOTE AS nome_pacote,
       VALOR_FINAL AS valor_final,
       DESC_CAMPANHA AS flag_desconto_campanha,
       VALOR_CAMPANHA_VENCIDO AS desconto_campanha_vencido,
       VALOR_CAMPANHA_A_VENCER AS desconto_campanha_a_vencer,
       CAST(DT_VENC_CAMPANHA AS DATE) AS data_vencimento_desconto_campanha,
       date_format(DT_VENC_CAMPANHA, 'MMM/yyyy') AS mes_vencimento_desconto_campanha,
       CAST(DATEADD(MONTH, 6, a.DT_VENC_CAMPANHA)AS DATE) AS data_vencimento_desconto_campanha_6m,
       DESC_OUTROS AS flag_outros_descontos,
       VALOR_OUTROS_VENCIDO AS desconto_outros_vencido,
       VALOR_OUTROS_A_VENCER AS desconto_outros_a_vencer,
       CAST(DT_VENC_OUTROS AS DATE) AS data_vencimento_desconto_outros,
       date_format(DT_VENC_OUTROS, 'MMM/yyyy') AS mes_vencimento_desconto_outros,
       CAST(DATEADD(MONTH, 6, a.DT_VENC_OUTROS) AS DATE) AS data_vencimento_desconto_outros_6m,
       date_format(CASE
               WHEN DATEADD(MONTH, 6, a.DT_VENC_CAMPANHA) IS NULL THEN DATEADD(MONTH, 6, a.DT_VENC_OUTROS)
               WHEN DATEADD(MONTH, 6, a.DT_VENC_OUTROS) IS NULL THEN DATEADD(MONTH, 6, a.DT_VENC_CAMPANHA)
               WHEN DATEADD(MONTH, 6, a.DT_VENC_CAMPANHA) >= DATEADD(MONTH, 6, a.DT_VENC_OUTROS) THEN DATEADD(MONTH, 6, a.DT_VENC_CAMPANHA)
               ELSE DATEADD(MONTH, 6, a.DT_VENC_OUTROS)
      END, 'MM/yyyy') AS ultimo_mes_desconto,
      DT_VENC_AJUSTE_COMERCIAL AS data_vencimento_desconto_ajuste_comercial,
      date_format(DT_VENC_AJUSTE_COMERCIAL, 'MMM/yyyy') AS mes_vencimento_desconto_ajuste_comercial,
      DT_VENC_RETENCAO AS data_vencimento_desconto_retencao,
      date_format(DT_VENC_RETENCAO, 'MMM/yyyy') AS mes_vencimento_desconto_retencao,
      DT_VENC_CORTESIA AS data_vencimento_desconto_cortesia,
      date_format(DT_VENC_CORTESIA, 'MMM/yyyy') AS mes_vencimento_desconto_cortesia,
      DT_VENC_AJUSTE_FINANCEIRO AS data_vencimento_desconto_ajuste_financeiro,
      date_format(DT_VENC_AJUSTE_FINANCEIRO, 'MMM/yyyy') AS mes_vencimento_desconto_ajuste_financeiro,
      CURRENT_DATE()-1 as data_referencia
FROM (
  SELECT t1.id_contrato_air AS CONTRATO,
         t5.nome AS STATUS_CONTRATO,
         t2.marca AS MARCA,
         t1.pacote_codigo AS CODIGO_PACOTE,
         t1.pacote_nome AS NOME_PACOTE,
         t1.valor_final AS VALOR_FINAL,
         MAX(CASE WHEN t3.categoria = 'CAMPANHA' THEN 1 ELSE 0 END) AS DESC_CAMPANHA,
         MAX(CASE WHEN t3.categoria = 'CAMPANHA' AND t3.desconto_ativo = 0 THEN t3.desconto ELSE NULL END) AS VALOR_CAMPANHA_VENCIDO,
         MAX(CASE WHEN t3.categoria = 'CAMPANHA' AND t3.desconto_ativo = 1 THEN t3.desconto ELSE NULL END) AS VALOR_CAMPANHA_A_VENCER,
         MAX(CASE WHEN t3.categoria = 'CAMPANHA' THEN t3.data_final_desconto ELSE NULL END) AS DT_VENC_CAMPANHA,
         MAX(CASE WHEN t4.categoria = 'OUTROS' THEN 1 ELSE 0 END) AS DESC_OUTROS,
         MAX(CASE WHEN t4.categoria = 'OUTROS' AND t4.desconto_ativo = 0 THEN t4.desconto ELSE NULL END) AS VALOR_OUTROS_VENCIDO,
         MAX(CASE WHEN t4.categoria = 'OUTROS' AND t4.desconto_ativo = 1 THEN t4.desconto ELSE NULL END) AS VALOR_OUTROS_A_VENCER,
         MAX(CASE WHEN t4.categoria = 'OUTROS' THEN t4.data_final_desconto ELSE NULL END) AS DT_VENC_OUTROS,
         MAX(CASE WHEN t6.categoria = 'AJUSTE COMERCIAL' THEN t6.data_final_desconto ELSE NULL END) AS DT_VENC_AJUSTE_COMERCIAL,
         MAX(CASE WHEN t6.categoria = 'RETENCAO' THEN t6.data_final_desconto ELSE NULL END) AS DT_VENC_RETENCAO,
         MAX(CASE WHEN t6.categoria = 'CORTESIA' THEN t6.data_final_desconto ELSE NULL END) AS DT_VENC_CORTESIA,
         MAX(CASE WHEN t6.categoria = 'AJUSTE FINANCEIRO' THEN t6.data_final_desconto ELSE NULL END) AS DT_VENC_AJUSTE_FINANCEIRO
  FROM gold.base.dim_contrato t1
  LEFT JOIN gold.base.dim_unidade t2 ON (t1.unidade_atendimento = t2.sigla AND t2.excluido = false AND t2.fonte = 'AIR')
  LEFT JOIN temp_desconto_consolidado t3 ON (t1.id_contrato_air = t3.id_contrato AND t3.categoria = 'CAMPANHA')
  LEFT JOIN temp_desconto_consolidado t4 ON (t1.id_contrato_air = t4.id_contrato AND t4.categoria = 'OUTROS')
  LEFT JOIN gold.base.dim_catalogo t5 ON (t1.status = t5.codigo AND t5.excluido = 0)
  LEFT JOIN temp_desconto_categoria t6 ON (t1.id_contrato = t6.id_contrato)
  WHERE t1.data_primeira_ativacao IS NOT NULL
    AND t1.data_cancelamento IS NULL
  GROUP BY t1.id_contrato_air, t5.nome, t2.marca, t1.pacote_codigo, t1.pacote_nome, t1.valor_final
) a
WHERE a.DESC_CAMPANHA = 1 OR a.DESC_OUTROS = 1;