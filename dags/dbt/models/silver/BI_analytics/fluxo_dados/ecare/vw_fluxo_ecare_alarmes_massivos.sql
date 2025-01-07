SELECT
  e.incident_id AS id,
  i.problem_identifier AS Id_Problem,
  o.serial AS ONU,
  u.sigla AS Unidade,
  u.nome AS Cidade,
  su.estado AS Estado, -- estado (inclusão coluna na {{ get_catalogo('gold') }}.base.dim_unidade),
  sr.macro_regional AS Regional,
  REPLACE(u.regional, 'R', 'T') AS Territorio,
  TRANSLATE(i.created_at, 'TZ', ' ') AS DataRegistroEcare, -- (confirmar se realmente é esse campo)
  eo.nature_type AS Tipo,
  i.status AS Status,
  i.quantity_onu_affected AS Afetados,
  o.olt,
  o.slot,
  o.pon
FROM {{ get_catalogo('gold') }}.ecare.dim_onu o
INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_event_onu eo 
  ON eo.onu_id = o.id
INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_event e 
  ON e.id = eo.event_id
INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_incident i 
  ON e.incident_id = i.id
INNER JOIN {{ get_catalogo('gold') }}.base.dim_conexao lg 
  ON lg.onu_serial = o.serial
INNER JOIN {{ get_catalogo('gold') }}.base.dim_contrato dc 
  ON dc.id_contrato = lg.contrato_codigo
INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_contrato_onu_aviso avisos 
  ON dc.id_contrato = avisos.id_contrato
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
  ON u.sigla = dc.unidade_atendimento
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade su
  ON dc.unidade_atendimento = su.sigla
LEFT JOIN {{ get_catalogo('gold') }}.auxiliar.subregionais sr
  ON sr.cidade = UPPER(u.nome)
WHERE 
  CAST(e.created_at AS DATE) >= '2024-07-01'
  AND CAST(e.created_at AS DATE) <= CURRENT_DATE() -1
  AND e.incident_id IS NOT NULL
  AND CAST(o.updated_at AS DATE) >= CAST(DATEADD(MONTH, -6, GETDATE()) AS DATE)
  AND dc.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
