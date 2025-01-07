SELECT DISTINCT
  a.data_venda,
  a.data_criacao,
  a.hora_criacao,
  a.id_contrato,
  a.id_cliente,
  a.tipo_cliente,
  a.segmento,
  a.equipe,
  a.canal,
  a.canal_tratado,
  a.tipo_canal,
  a.id_vendedor,
  a.nome_vendedor,
  a.unidade_atendimento,
  a.unidade,
  a.marca,
  a.estado,
  a.cidade,
  a.bairro,
  a.tipo_residencia,
  a.cep,
  a.latitude,
  a.longitude,
  a.regional,
  a.pacote_nome,
  a.velocidade,
  a.campanha_nome,
  a.produto,
  a.pacote_tv,
  a.cobranca,
  a.valor_total,
  a.pacote_valor_total,
  a.desconto,
  a.duracao_desconto,
  a.valor_final,
  a.afiliado_assine,
  a.data_extracao,
  a.email_vendedor,
  a.consultor_independente,
  a.fonte,
  a.legado_id,
  a.legado_sistema,
  a.id_contrato_air,
  a.id_contrato_ixc,
  a.valor_referencia_b2b,
  a.cupom,
  a.cupom_valido,
  REPLACE(
    COALESCE(
      {{ get_catalogo('silver') }}.stage_seeds_data.subregionais.subregional,

      u.cluster
    ),
    'CLUSTER_',
    ''
  ) AS cluster,
  a.macro_regional,
  a.nome_regional,
  a.cpf_cnpj
FROM
  (
    SELECT DISTINCT
      LEFT(v.data_venda, 10) AS data_venda,
      LEFT(v.data_criacao, 10) AS data_criacao,
      CASE
        WHEN v.fonte = 'AIR' THEN CAST(v.data_criacao AS TIMESTAMP)
        ELSE (
          CAST(v.data_extracao AS TIMESTAMP) - INTERVAL '5' MINUTE
        )
      END AS hora_criacao,
      v.id_contrato,
      v.id_cliente,
      v.tipo_cliente,
      CASE
        WHEN v.fonte = 'AIR' THEN v.segmento
        WHEN v.fonte = 'NG' THEN v.segmento
        ELSE cl.segmento
      END AS segmento,
      CASE
        WHEN v.fonte = 'AIR' THEN v.equipe
        ELSE v.canal
      END AS equipe,
      v.canal,
      v.canal_tratado,
      v.tipo_canal,
      v.id_vendedor,
      ve.nome AS nome_vendedor,
      v.unidade AS unidade_atendimento,
      CASE
        WHEN v.fonte = 'AIR' THEN COALESCE(v.unidade, e_c_air.unidade)
        ELSE COALESCE(v.unidade, e_cobranca.unidade)
      END AS unidade,
      v.marca,
      UPPER(
        CASE
          WHEN v.fonte = 'AIR' THEN COALESCE(v.estado, e_c_air.estado)
          ELSE COALESCE(v.estado, e_cobranca.estado)
        END
      ) AS estado,
      UPPER(
        CASE
          WHEN v.fonte = 'AIR' THEN COALESCE(UPPER(v.cidade), e_c_air.cidade)
          WHEN v.fonte <> 'AIR' THEN COALESCE(UPPER(v.cidade), e_cobranca.cidade)
        END
      ) AS cidade,
      UPPER(
        CASE
          WHEN v.fonte = 'AIR' THEN COALESCE(UPPER(v.bairro), e_c_air.bairro)
          WHEN v.fonte <> 'AIR' THEN COALESCE(UPPER(v.bairro), e_cobranca.bairro)
        END
      ) AS bairro,
      v.tipo_residencia,
      CASE
        WHEN v.fonte = 'AIR' THEN COALESCE(v.cep, e_c_air.cep)
        ELSE COALESCE(v.cep, e_cobranca.cep)
      END AS cep,
      CASE
        WHEN v.fonte = 'AIR' THEN COALESCE(v.latitude_entrega, e_c_air.latitude)
        ELSE COALESCE(v.latitude_entrega, e_cobranca.latitude)
      END AS latitude,
      CASE
        WHEN v.fonte = 'AIR' THEN COALESCE(v.longitude_entrega, e_c_air.longitude)
        ELSE COALESCE(v.longitude_entrega, e_cobranca.longitude)
      END AS longitude,
      v.regional,
      v.pacote_base_nome AS pacote_nome,
      v.velocidade,
      v.campanha_nome,
      v.produto,
      v.pacote_tv,
      v.cobranca,
      v.valor_total,
      v.pacote_valor_total,
      CASE
        WHEN v.fonte = 'AIR' THEN CAST(v.desconto AS DECIMAL(18, 2))
        ELSE CAST(
            v.recorrencia_percentual_desconto AS DECIMAL(18, 2)
          )
      END AS desconto,
      CAST(v.valor_final_com_desconto AS DECIMAL(18, 2)) AS valor_final,
      v.afiliado_assine,
      v.data_extracao,
      ve.email AS email_vendedor,
      v.consultor_autonomo AS consultor_independente,
      v.fonte,
      v.legado_id,
      v.legado_sistema,
      v.id_contrato_air,
      v.id_contrato_ixc,
      v.valor_referencia_b2b,
      v.cupom,
      v.cupom_valido,
      v.macro_regional,
      v.nome_regional,
      v.duracao_desconto,
      v.cpf_cnpj
    FROM
      {{ ref("dim_venda") }} AS v
    LEFT JOIN {{ ref("dim_vendedor") }} AS ve
      ON v.id_vendedor = ve.id_vendedor
      AND v.fonte = ve.fonte

    LEFT JOIN {{ ref('dim_cliente') }} AS cl

      ON v.id_cliente = cl.id_cliente
      AND v.fonte = cl.fonte

    LEFT JOIN {{ ref('dim_endereco') }} AS e_c_air

      ON v.id_endereco_cobranca = e_c_air.id_endereco
      AND v.fonte = e_c_air.fonte
      AND v.fonte = 'AIR'
    LEFT JOIN (
      SELECT *
      FROM

        {{ ref('dim_endereco') }}

      WHERE
        fonte <> 'AIR'
    ) AS e_cobranca ON (
      e_cobranca.logradouro = COALESCE(
        UPPER(v.endereco_cobranca),
        UPPER(v.endereco_entrega)
      )
      OR (
        e_cobranca.latitude = COALESCE(v.latitude_cobranca, v.latitude_entrega)
        OR e_cobranca.longitude = COALESCE(v.longitude_cobranca, v.longitude_entrega)
      )
    )
    AND v.fonte = e_cobranca.fonte
    AND v.id_cliente = e_cobranca.id_cliente
  ) AS a

LEFT JOIN {{ ref('dim_unidade') }} AS u

  ON CASE
    WHEN a.fonte = 'UNIVOX'
      AND a.unidade IS NULL THEN UPPER({{ translate_column('A.cidade') }})
    ELSE a.unidade
  END = CASE
    WHEN a.fonte = 'UNIVOX'
      AND a.unidade IS NULL THEN UPPER({{ translate_column('U.nome') }})
    ELSE u.sigla
  END

LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais

  ON UPPER({{ translate_column('A.cidade') }}) = {{ get_catalogo('silver') }}.stage_seeds_data.subregionais.cidade_sem_acento

  AND a.estado = UPPER({{ get_catalogo('silver') }}.stage_seeds_data.subregionais.uf)

WHERE
  TRY_CAST(a.data_venda AS DATE) < CAST(NOW() AS DATE)
