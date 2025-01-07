WITH login AS (
    SELECT
    login.contrato_codigo,
    login.usuario,
    login.habilitado
    FROM {{ ref('vw_air_tbl_login') }} AS login
    INNER JOIN (
        SELECT
        max(login.id) AS id
        FROM {{ ref('vw_air_tbl_login') }} AS login
        INNER JOIN {{ ref('vw_air_tbl_contrato') }} AS ctr ON ctr.id = login.contrato_codigo
        WHERE
        login.excluido = 0
        GROUP BY
        login.contrato_codigo
    ) AS A ON A.id = login.id
),

ult_fat_gerada AS (
    SELECT A.contrato_air,
    data_criacao as venc_ult_fat_gerada,
    status_fatura as status_ult_fat_gerada
    FROM (
      SELECT codigo_contrato_air as contrato_air,
      data_criacao,
      status_fatura,
      ROW_NUMBER() OVER(PARTITION BY codigo_contrato_air order by data_criacao desc) as rn
      FROM {{ ref('dim_faturas_all_versions') }}
      WHERE is_last = TRUE
    ) as A
    WHERE A.rn = 1
)

SELECT DATE_SUB(CURRENT_DATE(), 1) AS data_referencia,
A.id_contrato_air,
B.usuario AS login,
A.status as status_contrato,
C.venc_ult_fat_gerada,
C.status_ult_fat_gerada,
CASE
  WHEN A.status = 'ST_CONT_HABILITADO' THEN 1 ELSE 0 END AS habilitado

FROM {{ ref('dim_contrato') }} as A
LEFT JOIN login B ON A.id_contrato_air = B.contrato_codigo
LEFT JOIN ult_fat_gerada C ON A.id_contrato_air = C.contrato_air

UNION ALL

SELECT data_referencia,
id_contrato_air,
login,
status_contrato,
venc_ult_fat_gerada,
status_ult_fat_gerada,
habilitado
FROM {{ this }}
WHERE data_referencia >= DATE_SUB(CURRENT_DATE(), 100)
AND data_referencia < DATE_SUB(CURRENT_DATE(), 1)
