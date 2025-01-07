SELECT
  stg_contrato.id_contrato,
  unidade_atendimento_sigla,
  nome AS nome_cidade,
  codigo_ibge AS cidade_codigo_ibge,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_sydle AS stg_contrato
LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_unidade_atendimento AS contrato_unidade_atendimento
  ON stg_contrato.id_contrato = contrato_unidade_atendimento.id_contrato
