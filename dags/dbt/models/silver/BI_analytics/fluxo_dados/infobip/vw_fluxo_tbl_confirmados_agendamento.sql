SELECT
  a.id,
  CAST(a.data_confirmacao_agendamento AS DATE) AS data_confirmacao_agendamento
FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_os a
