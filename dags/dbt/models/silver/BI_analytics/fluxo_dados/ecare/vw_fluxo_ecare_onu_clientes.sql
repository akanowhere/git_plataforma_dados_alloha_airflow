SELECT
  DISTINCT CAST(oc.data_criacao AS DATE) AS Data,
  dc.id_cliente AS ID_Cliente,
  oc.contrato_codigo AS Id_Contrato,
  dc.status AS Status,
  oc.cliente_nome AS Nome_do_Cliente,
  sub.cidade_sem_acento AS Cidade,
  du.sigla AS Unidade,
  oc.Motivo_Provisionamento,
  oc.id_onu,
  do.serial
FROM
  {{ get_catalogo('gold') }}.ecare.dim_onu_configuracao oc
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato dc 
    ON dc.id_contrato = oc.contrato_codigo
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade du 
    ON du.area_distribuicao = dc.unidade_atendimento
  LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais sub 
    ON UPPER(sub.cidade) = UPPER(du.nome)
  INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_onu do 
    ON do.id = oc.id_onu
WHERE
  dc.status IN (
    'ST_CONT_HABILITADO',
    'ST_CONT_SUSP_CANCELAMENTO',
    'ST_CONT_SUSP_DEBITO',
    'ST_CONT_SUSP_SOLICITACAO'
  )
  AND sub.cidade IS NOT NULL