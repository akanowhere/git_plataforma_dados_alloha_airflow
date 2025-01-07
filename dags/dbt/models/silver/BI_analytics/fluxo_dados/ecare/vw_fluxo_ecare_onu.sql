WITH max_update_per_serial AS (
	SELECT
	    serial
	  , MAX(updated_at) AS max_update
	FROM {{ get_catalogo('gold') }}.ecare.dim_onu
	GROUP BY serial
)
SELECT DISTINCT
    a.serial
  , a.olt
  , a.status
  , a.unity
  , a.manufacture
  , CASE
        WHEN dc.status = 'ST_CONT_HABILITADO' THEN 'Habilitado'
        WHEN dc.status = 'ST_CONT_SUSP_CANCELAMENTO' THEN 'Suspenso p/ Cancelamento'
        WHEN dc.status = 'ST_CONT_SUSP_DEBITO' THEN 'Suspenso p/ Débito'
        WHEN dc.status = 'ST_CONT_SUSP_SOLICITACAO' THEN 'Suspenso p/ Solicitação'
        ELSE dc.status
    END AS status_contrato
  , CAST(a.created_at AS DATE) AS created_at
  , CAST(a.updated_at AS DATE) AS updated_at
FROM {{ get_catalogo('gold') }}.ecare.dim_onu a
INNER JOIN max_update_per_serial b
        ON a.serial = b.serial
       AND a.updated_at = b.max_update
INNER JOIN {{ get_catalogo('gold') }}.base.dim_conexao lg 
        ON lg.onu_serial = a.serial
INNER JOIN {{ get_catalogo('gold') }}.base.dim_contrato dc 
        ON dc.id_contrato = lg.contrato_codigo
WHERE a.serial NOT LIKE 'CMSZ;%'
  AND dc.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')