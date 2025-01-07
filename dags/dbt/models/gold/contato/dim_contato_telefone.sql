WITH cte_contato_inicial AS (
  SELECT
    t.id,
    CAST(t.data_criacao AS TIMESTAMP) AS data_criacao,
    CAST(t.data_alteracao AS TIMESTAMP) AS data_alteracao,
    CASE WHEN t.excluido = 'F' THEN '0' ELSE '1' END AS excluido,
    t.id_cliente,
    t.tipo,
    t.contato,
    CASE WHEN t.ativo = 'F' THEN '0' ELSE '1' END AS ativo,
    CASE WHEN t.confirmado = 'F' THEN '0' ELSE '1' END AS confirmado,
    t.status,
    CASE WHEN t.favorito = 'F' THEN '0' ELSE '1' END AS favorito,
    t.canal_confirmacao,
    CAST(t.data_criacao AS TIMESTAMP) AS data_alter_contato,
    CAST(t.data_confirmacao AS TIMESTAMP) AS data_confirmacao,
    t.origem,
    CASE WHEN w.origin IS NULL THEN 'AIR' ELSE w.origin END AS fonte_contato,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
    'S' AS existe_air,
    CASE WHEN w.autorizado IS NULL THEN NULL ELSE autorizado END AS autorizado
  FROM
    {{ get_catalogo('silver') }}.stage_contato.vw_contato_telefone AS t -- o schema não pode ser contato, precisa ser stage_contato
  LEFT JOIN (
    SELECT DISTINCT
      RIGHT(a.phone, LEN(a.phone) - 1) AS phone,
      customer_id,
      origin,
      CASE WHEN authorized = 'F' THEN '0' ELSE '1' END AS autorizado
    FROM
      {{ get_catalogo('silver') }}.stage_contato.vw_contato_whatsapp AS a
    INNER JOIN (
      SELECT
        MAX(id) AS id,
        phone
      FROM
        {{ get_catalogo('silver') }}.stage_contato.vw_contato_whatsapp
      GROUP BY
        phone
    ) AS b ON a.id = b.id
  ) AS w ON t.id_cliente = w.customer_id AND t.contato = w.phone

  UNION

  SELECT
    w.id,
    CAST(w.created_at AS TIMESTAMP) AS data_criacao,
    CAST(w.updated_at AS TIMESTAMP) AS data_alteracao,
    CASE WHEN w.deleted = 'F' THEN '0' ELSE '1' END AS excluido,
    w.customer_id AS id_cliente,
    NULL AS tipo,
    w.phone AS contato,
    NULL AS ativo,
    NULL AS confirmado,
    NULL AS status,
    CASE WHEN w.favorite = 'F' THEN '0' ELSE '1' END AS favorito,
    NULL AS canal_confirmacao,
    NULL AS data_alter_contato,
    NULL AS data_confirmacao,
    w.origin AS origem,
    CASE WHEN w.origin IS NULL THEN 'AIR' ELSE w.origin END AS fonte_contato,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
    'N' AS existe_air,
    autorizado
  FROM
    {{ get_catalogo('silver') }}.stage_contato.vw_contato_telefone AS t
  RIGHT JOIN (
    SELECT DISTINCT
      RIGHT(a.phone, LEN(a.phone) - 1) AS phone,
      customer_id,
      origin,
      a.id,
      created_at,
      updated_at,
      deleted,
      favorite,
      CASE WHEN authorized = 'F' THEN '0' ELSE '1' END AS autorizado
    FROM
      {{ get_catalogo('silver') }}.stage_contato.vw_contato_whatsapp AS a
    INNER JOIN (
      SELECT
        MAX(id) AS id,
        phone
      FROM
        {{ get_catalogo('silver') }}.stage_contato.vw_contato_whatsapp
      GROUP BY
        phone
    ) AS b ON a.id = b.id
  ) AS w ON t.id_cliente = w.customer_id AND t.contato = w.phone
  WHERE
    t.id_cliente IS NULL
),
cte_telefone_tratado AS (
  SELECT
    cc.id,
    cc.contato,
    CASE
      WHEN LEN(contato_sem_55) = 10
        AND CAST(SUBSTRING(RIGHT(contato_sem_55, 8), 1, 1) AS VARCHAR(20)) IN ('6', '7', '8', '9')
        THEN CONCAT(SUBSTRING(contato_sem_55, 1, 2), '9', RIGHT(contato_sem_55, 8))
      ELSE contato_sem_55
    END AS telefone_tratado,
    cc.existe_air
  FROM
    cte_contato_inicial AS cc
  LEFT JOIN (
    SELECT
      t.id,
      CASE
        WHEN LENGTH(SUBSTRING(
            REPLACE(REPLACE(t.contato, '(', ''), ')', ''),
            REGEXP_INSTR(REPLACE(REPLACE(t.contato, '(', ''), ')', ''), '[1-9]'),
            LENGTH(t.contato)
          )) BETWEEN 12 AND 13
          THEN (CASE
            WHEN SUBSTRING(SUBSTRING(
                REPLACE(REPLACE(t.contato, '(', ''), ')', ''),
                REGEXP_INSTR(REPLACE(REPLACE(t.contato, '(', ''), ')', ''), '[1-9]'),
                LENGTH(t.contato)
              ), 1, 2) = '55'
              THEN SUBSTRING(SUBSTRING(
                  REPLACE(REPLACE(t.contato, '(', ''), ')', ''),
                  REGEXP_INSTR(REPLACE(REPLACE(t.contato, '(', ''), ')', ''), '[1-9]'),
                  LENGTH(t.contato)
                ), 3, LENGTH(t.contato
                ))
            ELSE SUBSTRING(
                REPLACE(REPLACE(t.contato, '(', ''), ')', ''),
                REGEXP_INSTR(REPLACE(REPLACE(t.contato, '(', ''), ')', ''), '[1-9]'),
                LENGTH(t.contato)
              )
          END)
        ELSE SUBSTRING(
            REPLACE(REPLACE(t.contato, '(', ''), ')', ''),
            REGEXP_INSTR(REPLACE(REPLACE(t.contato, '(', ''), ')', ''), '[1-9]'),
            LENGTH(t.contato)
          )
      END AS contato_sem_55,
      t.contato,
      t.id_cliente,
      t.data_extracao,
      t.existe_air
    FROM
      cte_contato_inicial AS t
    WHERE
      t.contato NOT LIKE '%[A-Z]%'
      AND t.contato IS NOT NULL
      AND t.id IS NOT NULL
  ) AS tb_contato_sem_55 ON cc.id = tb_contato_sem_55.id AND cc.existe_air = tb_contato_sem_55.existe_air
  WHERE
    cc.contato NOT LIKE '%[A-Z]%'
    AND cc.contato IS NOT NULL
    AND cc.id IS NOT NULL
),
cte_contato_tratado AS (
  SELECT
    t.*,
    tt.telefone_tratado,
    CASE
      WHEN (
        (RIGHT(tt.telefone_tratado, 8) = '00000000')
        OR (RIGHT(tt.telefone_tratado, 8) = '11111111')
        OR (RIGHT(tt.telefone_tratado, 8) = '22222222')
        OR (RIGHT(tt.telefone_tratado, 8) = '33333333')
        OR (RIGHT(tt.telefone_tratado, 8) = '44444444')
        OR (RIGHT(tt.telefone_tratado, 8) = '55555555')
        OR (RIGHT(tt.telefone_tratado, 8) = '66666666')
        OR (RIGHT(tt.telefone_tratado, 8) = '77777777')
        OR (RIGHT(tt.telefone_tratado, 8) = '88888888')
        OR (RIGHT(tt.telefone_tratado, 8) = '99999999')
      )
        THEN 'NAO IDENTIFICADO'
      WHEN (
        CAST(SUBSTRING(RIGHT(tt.telefone_tratado, 9), 1, 1) AS VARCHAR(30)) = '9'
        AND LEN(tt.telefone_tratado) = 11
      )
        THEN 'MOVEL'
      WHEN LEN(tt.telefone_tratado) = 10
        THEN 'FIXO'
      WHEN LEN(tt.telefone_tratado) NOT IN ('10', '11')
        THEN 'NAO IDENTIFICADO'
      ELSE 'NAO IDENTIFICADO'
    END AS classificacao_contato
  FROM
    cte_contato_inicial AS t
  INNER JOIN
    cte_telefone_tratado AS tt ON t.id = tt.id AND t.existe_air = tt.existe_air
)
SELECT *
FROM
  cte_contato_tratado
