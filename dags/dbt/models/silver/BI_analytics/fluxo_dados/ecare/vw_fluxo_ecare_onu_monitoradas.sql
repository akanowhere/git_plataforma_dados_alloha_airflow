WITH max_update_per_serial AS (
	SELECT
	    serial
	  , MAX(updated_at) AS max_update
	FROM {{ get_catalogo('gold') }}.ecare.dim_onu
	GROUP BY serial
),
sub AS (
	SELECT
	    a.serial
	  , CASE
            WHEN a.status = 'sem_energia' THEN 'falta_energia'
            ELSE a.status
        END AS status
	  , a.unity
	  , a.manufacture
	  , a.created_at
	  , a.updated_at
	FROM {{ get_catalogo('gold') }}.ecare.dim_onu a
	INNER JOIN max_update_per_serial b
	        ON a.serial = b.serial
	       AND a.updated_at = b.max_update
)
SELECT 
    sub.serial
  , MIN(sub.created_at) AS data_criacao
  , sub.manufacture AS fabricante_onu
  , sub.unity AS Unidade
  , sub.status
FROM {{ get_catalogo('gold') }}.base.dim_contrato ct
  JOIN {{ get_catalogo('gold') }}.ecare.dim_contrato_onu_aviso avisos 
    ON ct.id_contrato = avisos.id_contrato
  JOIN {{ get_catalogo('gold') }}.base.dim_conexao lg 
    ON lg.contrato_codigo = ct.id_contrato
  JOIN sub
    ON lg.onu_serial = sub.serial
WHERE 1=1
    AND CAST(sub.updated_at AS DATE) >= CAST(DATEADD(MONTH, -6, GETDATE()) AS DATE)
    AND ct.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
GROUP BY sub.serial
       , sub.manufacture
       , sub.unity
       , sub.status