SELECT
  a.id,
  a.protocolo,
  au.unidades AS unidade,
  u.nome AS cidade,
  su.estado, -- estado (inclusão coluna na {{ get_catalogo('gold') }}.base.dim_unidade),
  sr.macro_regional AS regional,
  REPLACE(u.regional, 'R', 'T') AS territorio,
  TRANSLATE(a.data_criacao, 'TZ', ' ') AS data_criacao,
  TRANSLATE(a.data_inicio, 'TZ', ' ') AS data_inicio,
  TRANSLATE(a.data_fim, 'TZ', ' ') AS data_fim,
  a.usuario_criacao,
  a.ura_ativa,
  sa.vigente, -- vigente (inclusão coluna na {{ get_catalogo('gold') }}.ecare.dim_aviso),
  a.equipe_responsavel,
  a.id_problema,
  a.clientes_afetados,
  a.nivel_criticidade,
  a.afeta_internet,
  a.afeta_telefonia,
  a.afeta_tv,
  a.falha_energia,
  a.ocorrencia,
  a.id_os_rede_externa,
  a.tempo_solucao,
  a.proxima_atualizacao,
  a.impacto,
  a.usuario_alteracao,
  TRANSLATE(a.data_alteracao, 'TZ', ' ') AS data_alteracao,
  aa.descricao AS atividades
FROM {{ get_catalogo('gold') }}.ecare.dim_aviso a
  LEFT JOIN {{ get_catalogo('gold') }}.ecare.dim_aviso_unidade au
    ON a.id = au.id_aviso
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u
    ON au.unidades = u.sigla
  LEFT JOIN {{ get_catalogo('gold') }}.ecare.dim_aviso_atividade aa
    ON a.id = aa.id_aviso
  LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade su
    ON au.unidades = su.sigla
  LEFT JOIN {{ get_catalogo('gold') }}.auxiliar.subregionais sr
    ON sr.cidade = UPPER(u.nome)
  LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_aviso sa
    ON a.id = sa.id