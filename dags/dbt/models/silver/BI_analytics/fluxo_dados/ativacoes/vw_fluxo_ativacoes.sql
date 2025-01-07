WITH cte_ativacoes AS (
  SELECT DISTINCT 
    a.id_contrato,
    a.CODIGO_CONTRATO_AIR,
    a.CODIGO_CONTRATO_IXC,
    CAST(a.data_ativacao AS DATE) AS data_ativacao,
    DATE_FORMAT(d.data_conclusao, 'HH:mm:ss') AS hora_ativacao,
    UPPER(
      TRANSLATE(
        a.cidade,
        "ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ",
        "AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao"
      )
    ) AS cidade,
    UPPER(a.estado) AS estado,
    b.equipe,
    b.canal_tratado AS canal,
    b.nome_vendedor,
    a.marca,
    a.unidade,
    a.regional,
    a.produto,
    a.segmento,
    a.motivo_inclusao,
    CAST(a.valor_contrato AS DOUBLE) AS valor_contrato,
    a.plano,
    a.legado_sistema,
    a.fonte,
    a.pacote_tv
  FROM
    {{ get_catalogo('gold') }}.base.fato_primeira_ativacao a
    LEFT JOIN {{ get_catalogo('gold') }}.venda.fato_venda b 
      ON a.marca = b.marca
      AND COALESCE(a.CODIGO_CONTRATO_IXC, a.CODIGO_CONTRATO_AIR) = COALESCE(b.id_contrato_air, b.id_contrato_ixc)
    LEFT JOIN (
      SELECT
        contrato_codigo,
        data_conclusao,
        codigo_fila,
        motivo_conclusao,
        ROW_NUMBER() OVER (PARTITION BY contrato_codigo, CAST(data_conclusao as DATE) ORDER BY data_conclusao DESC) AS rn
      FROM
        {{ get_catalogo('gold') }}.chamados.dim_os
      WHERE
        data_conclusao IS NOT NULL
        AND codigo_fila = 'ES02'
        AND motivo_conclusao LIKE '%EXECUTADO%'
    ) d 
    ON d.contrato_codigo = a.CODIGO_CONTRATO_AIR
    AND d.rn = 1
    AND CAST(a.data_ativacao AS DATE) = CAST(d.data_conclusao AS DATE)
  WHERE
    a.data_ativacao >= '2023-01-01'
)
SELECT DISTINCT 
  a.id_contrato,
  a.CODIGO_CONTRATO_AIR,
  a.CODIGO_CONTRATO_IXC,
  a.data_ativacao,
  a.hora_ativacao,
  a.cidade,
  a.estado,
  a.equipe,
  a.canal,
  a.nome_vendedor,
  a.marca,
  a.unidade,
  a.regional,
  COALESCE(sr.subregional, 'Sem Classificacao') AS cluster,
  a.produto,
  a.segmento,
  a.motivo_inclusao,
  a.valor_contrato,
  a.plano,
  a.legado_sistema,
  a.fonte,
  a.pacote_tv
FROM
  cte_ativacoes a
  LEFT JOIN silver.stage_seeds_data.subregionais sr 
    ON a.cidade = UPPER(sr.cidade_sem_acento)
    AND a.estado = sr.uf