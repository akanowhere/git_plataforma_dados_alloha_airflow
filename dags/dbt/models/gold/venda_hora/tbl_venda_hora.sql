SELECT
  a.data_criacao,
  a.id_contrato_air,
  a.equipe,
  UPPER(CASE
    WHEN consultor_autonomo = 1
      THEN 'GIGA EMBAIXADOR'
    WHEN a.cupom_valido = 1 
      THEN 'INDIQUE UM AMIGO'
    ELSE canal_tratado
  END) AS canal_tratado,
  a.valor_final,
  a.fonte,
  a.marca,
  a.regional,
  a.macro_regional,
  a.nome_regional,
  a.legado_id,
  a.legado_sistema,
  a.cupom,
  a.cupom_valido,
  a.segmento,
  a.data_extracao,
  a.cidade,
  a.estado
FROM (
  SELECT
    CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    vh.id_contrato_air,
    equipe,
    CASE
      WHEN canal = 'TLV RECEPTIVO'
        THEN 'RECEPTIVO'
      ELSE canal
    END AS canal_tratado,
    CASE
      WHEN (
        SELECT MAX(aa.contract_air_id)
        FROM
          {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_consultor AS aa
        WHERE
          aa.contract_air_id = vh.id_contrato_air
      ) IS NOT NULL
        THEN 1
      ELSE 0
    END AS consultor_autonomo,
    valor_final,
    fonte,
    vh.marca,
    COALESCE(CAST(subregionais.sigla_regional AS VARCHAR(100)), vh.regional) AS regional,
    subregionais.macro_regional,
    subregionais.regional1 AS nome_regional,
    legado_id,
    legado_sistema,
    UPPER(ac.cupom) AS cupom,
    CASE
      WHEN (UPPER(ac.cupom)) IN (
          SELECT UPPER(cupom)
          FROM
            {{ get_catalogo('silver') }}.stage_venda_hora.vw_air_cupom
        )
        THEN 1
      ELSE 0
    END AS cupom_valido,
    segmento,
    data_extracao,
    vh.cidade,
    vh.estado
  FROM
    {{ ref('vw_venda_hora') }} AS vh
  LEFT JOIN
    {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_cupom AS ac
    ON vh.id_contrato_air = ac.contrato_air
  LEFT JOIN
    (
      SELECT DISTINCT
        cidade_sem_acento,
        sigla_regional,
        macro_regional,
        regional1,
        uf
      FROM {{ get_catalogo('silver') }}.stage_seeds_data.subregionais
    ) AS subregionais
    ON TRANSLATE(UPPER(vh.cidade), 'ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃ', 'AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnA') = UPPER(subregionais.cidade_sem_acento)
    AND vh.estado = UPPER(subregionais.uf)
  WHERE (segmento <> 'B2B' OR segmento IS NULL)
    AND (canal NOT IN ('MIGRACAO'))
) AS a
