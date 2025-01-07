SELECT DISTINCT 

      cast(w.send_at as TIMESTAMP) DataHora_Disparo,
      cast(w.send_at as DATE) DATA,
      W.message_id Message_id_disparo,
      W.status Status_disparo,
      C.message_id message_id_Agendada,
      C.id_os,
      r.id,
      C.id_cliente,
      c.id_contrato,
    CASE
      WHEN o.fila LIKE '%REPARO%' THEN 'REPARO'
      WHEN o.fila like '%ATIVAÇÃO%' THEN 'ATIVAÇÃO'
      ELSE O.FILA
    END AS Fila,
      O.unidade,
      CA.data_confirmacao_agendamento,
      Geral.origem_confirmacao_agendamento,
    CASE
      WHEN R.observacao = 'Reagendamento Chat' THEN 'Reagendado'
      WHEN CA.motivo_conclusao = 'CANCEL_CANCELAMENTO_CLIENTE_APP' THEN 'Cancelado'	
      WHEN CA.data_confirmacao_agendamento IS NOT NULL THEN 'Confirmado'
      WHEN CA.data_confirmacao_agendamento IS NULL THEN 'Não Confirmado'
      ELSE O.conclusao
    END status_confirmacao
    

FROM {{ get_catalogo('gold') }}.infobip.fato_disparos_whatsapp W

LEFT JOIN {{ get_catalogo('gold') }}.infobip.dim_os_notificacao_confirmacao C  
  ON C.message_id = W.message_id

LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_ordem O  
  ON O.codigo_os = C.id_os

LEFT JOIN (
            SELECT DISTINCT
                a.id,
                CAST(a.data_confirmacao_agendamento AS DATE) AS data_confirmacao_agendamento,
                a.motivo_conclusao
          FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_os a )  CA
  ON CA.id = C.id_os

LEFT JOIN (
            SELECT DISTINCT
                    CAST(data_criacao AS DATE) Data,
                    id,
                    servico,
                    observacao
            FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_os 
            WHERE  
                observacao = 'Reagendamento Chat' ) R
    ON R.id = C.id_os
LEFT JOIN (
    SELECT
      id,
      origem_confirmacao_agendamento
    FROM
      {{ get_catalogo('silver') }}.stage.vw_air_tbl_os
) Geral ON Geral.id = C.id_os

WHERE 
  CAST(w.send_at as DATE) <= CURRENT_DATE - INTERVAL 1 DAY
  AND C.message_id IS NOT NULL
  AND w.traffic_source = 'API'

  




