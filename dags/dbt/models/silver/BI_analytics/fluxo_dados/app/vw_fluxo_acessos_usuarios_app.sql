SELECT
  DISTINCT s.createdAt DataHora,
  s.sessions_id,
  s.document Documento,
  s.brand Marca,
  s.os Plataforma,
  s.source Origem,
  e.contract_number IdContrato
FROM
   {{ get_catalogo('gold') }}.app.fato_sessions s
  LEFT JOIN  {{ get_catalogo('gold') }}.app.fato_events e ON e.session_id = s.sessions_id