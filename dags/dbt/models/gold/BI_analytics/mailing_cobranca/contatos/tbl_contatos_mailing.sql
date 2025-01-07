{{ 
    config(
        materialized='table'
    ) 
}}

SELECT 
  dct.id_cliente,
  MAX(tel_01) AS tel_01,
  MAX(tel_02) AS tel_02,
  MAX(tel_03) AS tel_03,
  MAX(tel_04) AS tel_04,
  MAX(tel_05) AS tel_05,
  MAX(tel_06) AS tel_06,
  MAX(whatsapp) AS whatsapp,
  MAX(s.telefone) AS sms,
  MAX(email) AS email
FROM  {{ ref('dim_contato_telefone') }} dct
LEFT JOIN {{ ref('tmp_telefones') }} AS PivotData
  ON dct.id_cliente = PivotData.id_cliente
LEFT JOIN {{ ref('tmp_whatsapp') }} AS ws
  ON dct.id_cliente = ws.id_cliente
LEFT JOIN {{ ref('tmp_email') }} AS e
  ON dct.id_cliente = e.id_cliente AND e.prioridade = 1
LEFT JOIN {{ ref('tmp_sms') }} AS s
  ON dct.id_cliente = s.id_cliente
GROUP BY dct.id_cliente