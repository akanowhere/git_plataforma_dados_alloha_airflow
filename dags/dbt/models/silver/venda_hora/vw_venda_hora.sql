SELECT DISTINCT
  DATE_FORMAT(tvendas.data_criacao, 'yyyy-MM-dd HH:mm') AS data_criacao,
  tvendas.id_contrato AS id_contrato_air,
  REPLACE(tvendas.equipe, 'EQUEQUIPE_', 'EQUIPE_') AS equipe,
  CASE
    WHEN tvendas.equipe IN ('EQUIPE_PAP', 'EQUIPE_PAP_BRASILIA') THEN 'PAP DIRETO'
    WHEN tvendas.equipe = 'EQUIPE_TLV_ATIVO' THEN 'ATIVO'
    WHEN tvendas.equipe IN (
        'EQUIPE_AVANT_TELECOM_EIRELI', 'EQUIPE_BLISS_TELECOM', 'EQUIPE_FIBRA_TELECOM',
        'EQUIPE_RR_TELECOM', 'EQUIPE_MAKOTO_TELECOM', 'EQUIPE_MORANDI', 'EQUIPE_LITORAL_SAT'
      ) THEN 'PAP INDIRETO'
    WHEN tvendas.equipe LIKE '%PAP%' THEN 'PAP INDIRETO'
    WHEN tvendas.equipe IN ('AGENTE_DIGITAL_DIRETO', 'DIGITAL_ALLOHA') THEN 'CHATBOT'
    WHEN tvendas.equipe LIKE '%AGENTE_DIGITAL%' THEN 'AGENTE DIGITAL'
    WHEN tvendas.equipe LIKE '%DIGITAL_ASSINE%' THEN 'ASSINE'
    WHEN (
      tvendas.equipe LIKE 'EQUIPE_DIGITAL'
      OR tvendas.equipe LIKE 'EQUIPE_DIGITAL_BRA%'
      OR tvendas.equipe LIKE '%COM_DIGITAL%'
      OR tvendas.equipe LIKE 'DIGITAL'
    ) THEN 'DIGITAL'
    WHEN tvendas.equipe LIKE '%LOJA%' THEN 'LOJA'
    WHEN tvendas.equipe LIKE '%OUTBOUND%' THEN 'OUTBOUND'
    WHEN tvendas.equipe LIKE '%OUVIDORIA%' THEN 'OUVIDORIA'
    WHEN tvendas.equipe LIKE '%RETENCAO%' THEN 'RETENCAO'
    WHEN tvendas.equipe LIKE '%CORPORATIVO%' THEN 'CORPORATIVO'
    WHEN tvendas.equipe LIKE '%COBRANCA%' THEN 'COBRANCA'
    WHEN tvendas.equipe LIKE '%MIGRACAO%' THEN 'MIGRACAO'
    WHEN tvendas.equipe LIKE '%RH%' THEN 'RH'
    WHEN tvendas.equipe LIKE '%TLV_ATIVO%' THEN 'ATIVO'
    WHEN (
      tvendas.equipe LIKE '%RECEP%'
      OR tvendas.equipe LIKE '%CALL_VENDAS%'
      OR tvendas.equipe LIKE '%TELEVENDAS%'
    ) THEN 'TLV RECEPTIVO'
    WHEN tvendas.equipe LIKE '%PRODUTOS_MKT%'
      OR tvendas.equipe = 'MARKETING'  THEN 'MARKETING'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_CLICK%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_UNIVOX%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_INDEPENDENTE%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_GIGA_EMBAIXADOR%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE 'EQUIPE_GIGA_EMBAIXADOR_DIGITAL'  THEN 'GIGA EMBAIXADOR DIGITAL'
    ELSE 'SEM ALOCAÇÃO'
  END AS canal,
  ROUND(tvendas.valor_total - (COALESCE(tvendas.pacote_valor_total * (COALESCE(tvendas.recorrencia_percentual_desconto, tdesconto.desconto)), 0) / 100), 2) AS valor_final,
  'AIR' AS fonte,
  UPPER(tunidade.marca) AS marca,
  UPPER(tunidade.regional) AS regional,
  tcontrato.legado_id,
  tcontrato.legado_sistema,
  tendereco_cidade.nome AS cidade,
  tendereco_cidade.estado,
  CASE
    WHEN tcontrato.b2b = 'true' THEN 'B2B'
    WHEN tcontrato.pme = 'true' THEN 'PME'
    ELSE 'B2C'
  END AS segmento,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM
  {{ ref('vw_air_tbl_venda') }} AS tvendas
LEFT JOIN {{ ref('vw_air_tbl_contrato') }} AS tcontrato
  ON (tvendas.id_contrato = tcontrato.id)
LEFT JOIN {{ ref('vw_air_tbl_cliente') }} AS tcliente
  ON (tcontrato.id_cliente = tcliente.id)
LEFT JOIN {{ ref('vw_air_tbl_endereco') }} AS tendereco
  ON (tvendas.id_endereco = tendereco.id)
LEFT JOIN {{ ref('vw_air_tbl_endereco_cidade') }} AS tendereco_cidade
  ON (tendereco.id_cidade = tendereco_cidade.id)
LEFT JOIN {{ ref('vw_air_tbl_unidade') }} AS tunidade
  ON (
    tendereco.unidade = tunidade.sigla
    AND tunidade.excluido = 'false'
  )
LEFT JOIN {{ ref('vw_air_tbl_desconto') }} AS tdesconto
  ON (
    tvendas.id_contrato = tdesconto.id_contrato
    AND tvendas.pacote_base_codigo = tdesconto.item_codigo
    AND tdesconto.categoria = 'CAMPANHA'
  )
WHERE
  tvendas.natureza = 'VN_NOVO_CONTRATO'
  AND tvendas.equipe <> 'EQUIPE_MIGRACAO'
  AND tvendas.id_contrato IS NOT NULL
  AND (tcontrato.legado_sistema NOT IN (
    'ixc_click', 'integrator_univox', 'ixc_univox', 'protheus_ligue', 'ng_vip',
    'mk_niu', 'ng_niu', 'ng_pamnet', 'mk_pamnet', 'adapter', 'URBE'
  ) OR tcontrato.legado_sistema IS NULL)
  AND (tcontrato.data_criacao >= '2023-01-01' OR tcontrato.status <> 'ST_CONT_CANCELADO')
  AND TRIM(tcliente.nome) NOT LIKE '%SUMICITY%'
  AND TRIM(tcliente.nome) NOT LIKE '%VM OPENLINK%'
  AND TRIM(tcliente.nome) NOT LIKE '%VELOMAX%'
  AND CAST(tvendas.data_criacao AS DATE) >= CAST(CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) - INTERVAL '7' DAY AS DATE)
