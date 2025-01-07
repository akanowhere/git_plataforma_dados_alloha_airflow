SELECT
  DISTINCT CAST(eo.created_at AS DATE) AS Data,
  CAST(eo.created_at AS TIMESTAMP) AS created_at,
  u.nome AS Cidade,
  u.sigla AS Unidade,
  lg.onu_serial AS serial
FROM
  {{ get_catalogo('gold') }}.ecare.dim_onu o
  INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_event_onu eo 
    ON eo.onu_id = o.id
  INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_event e 
    ON e.id = eo.event_id
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_conexao lg 
    ON lg.onu_serial = o.serial
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_contrato dc 
    ON dc.id_contrato = lg.contrato_codigo
  INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_contrato_onu_aviso avisos 
    ON dc.id_contrato = avisos.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
    ON u.sigla = dc.unidade_atendimento
WHERE
  CAST(eo.created_at AS DATE) >= '2024-07-01'
  AND eo.nature_type = 'PROBLEM'
  AND e.incident_id IS NULL
  AND CAST(o.updated_at AS DATE) >= CAST(DATEADD(MONTH, -6, GETDATE()) AS DATE)
  AND dc.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')