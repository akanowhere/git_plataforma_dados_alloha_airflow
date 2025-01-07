SELECT
  hist.ID_FATURA_NUMERO,
  hist.ststatusvalidacaociclo,
  hist.flvalorcalculofinal
FROM {{ source('db_financeiro', 'TBL_VALIDACAO_CICLO_AUX_HISTORICO') }} AS hist
INNER JOIN (
  SELECT
    ID_FATURA_NUMERO,
    MAX(ID_VALIDACAOCICLO) AS id_max
  FROM {{ source('db_financeiro', 'TBL_VALIDACAO_CICLO_AUX_HISTORICO') }}
  GROUP BY ID_FATURA_NUMERO
) AS tb_temp ON hist.ID_FATURA_NUMERO = tb_temp.ID_FATURA_NUMERO
AND hist.ID_VALIDACAOCICLO = tb_temp.id_max
