SELECT
  DISTINCT CONCAT(SUBSTRING(s.createdAt, 4, 2), '/' ,SUBSTRING(s.createdAt, 7, 4)) MesAno,
  s.brand Marca,
  e.label Categoria_Evento,
  COUNT(e.session_id) AS Qtd
FROM
  {{ get_catalogo('gold') }}.app.fato_sessions s
  LEFT JOIN {{ get_catalogo('gold') }}.app.fato_events e 
    ON e.session_id = s.sessions_id
WHERE TO_TIMESTAMP(e.createdAt, 'dd-MM-yyyy HH:mm:ss') >= '2024-01-01 00:00:00'
  AND TO_TIMESTAMP(e.createdAt, 'dd-MM-yyyy HH:mm:ss') <= '2024-06-30 23:59:59'
GROUP BY CONCAT(SUBSTRING(s.createdAt, 4, 2), '/' ,SUBSTRING(s.createdAt, 7, 4)),
        s.brand,
        e.label