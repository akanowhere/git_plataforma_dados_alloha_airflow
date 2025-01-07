SELECT
  lg.onu_serial AS ONU,
  u.sigla AS Unidade,
  u.nome AS Cidade,
  su.estado AS Estado,
  sr.macro_regional AS Regional,
  REPLACE(u.regional, 'R', 'T') AS Territorio
FROM {{ get_catalogo('gold') }}.base.dim_conexao lg 
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
    dc.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
