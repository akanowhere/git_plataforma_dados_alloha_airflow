WITH monitorada AS (
  SELECT DISTINCT
    lg.onu_serial AS Serial,
    'Sim' AS Monitorada
  FROM {{ get_catalogo('gold') }}.base.dim_conexao lg
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato ct
    ON lg.contrato_codigo = ct.id_contrato
  WHERE 1=1
    AND ct.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
    AND lg.possui_circuit_id = TRUE
),
provisionada AS (
  SELECT
    lg.onu_serial AS Serial,
    oc.pppoe_login AS PPPoE,
    'Sim' AS Provisionada
  FROM {{ get_catalogo('gold') }}.base.dim_conexao lg
    INNER JOIN {{ get_catalogo('gold') }}.ecare.dim_onu_configuracao oc
      ON lg.contrato_codigo = oc.contrato_codigo
    INNER JOIN {{ get_catalogo('gold') }}.base.dim_contrato c
      ON lg.contrato_codigo = c.id_contrato
  WHERE 
    c.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
    AND oc.status_integracao = 'CONFIGURADA'
    AND oc.vigente = TRUE
)
SELECT
  lg.onu_serial AS Serial,
  o.olt,
  o.slot,
  o.pon,
  o.manufacture,
  CASE
    WHEN m.Monitorada IS NULL THEN 'Não'
    ELSE m.Monitorada
  END AS Monitorada,
  CASE
    WHEN p.Provisionada IS NULL THEN 'Não'
    ELSE p.Provisionada
  END AS Provisionada,
  p.PPPoE,
  lg.wifi_nome AS NomeRede,
  TRANSLATE(o.updated_at, 'TZ', ' ') AS UltimoAlarme,
  o.status AS Alarme,
  ct.id_cliente AS Cliente,
  ct.id_contrato AS Contrato,
  ct.status AS StatusContrato,
  ct.unidade_atendimento AS Regiao,
  su.estado AS Estado,
  su.nome AS Cidade,
  sr.macro_regional AS Regional,
  REPLACE(u.regional, 'R', 'T') AS Territorio,
  ct.data_primeira_ativacao AS DataAtivacao
FROM {{ get_catalogo('gold') }}.base.dim_conexao lg
  LEFT JOIN {{ get_catalogo('gold') }}.ecare.dim_onu o
    ON lg.onu_serial = o.serial
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato ct
    ON lg.contrato_codigo = ct.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u
    ON ct.unidade_atendimento = u.sigla
  LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade su
    ON ct.unidade_atendimento = su.sigla
  LEFT JOIN {{ get_catalogo('gold') }}.auxiliar.subregionais sr
    ON sr.cidade = UPPER(u.nome)
  LEFT JOIN monitorada m
    ON lg.onu_serial = m.Serial
  LEFT JOIN provisionada p
    ON lg.onu_serial = p.Serial
WHERE lg.onu_serial IS NOT NULL