SELECT
  MAX(id) AS id,
  data,
  usuario,
  contrato,
  tipo_solicitacao,
  ferramenta_retencao,
  motivo,
  submotivo,
  cidade,
  estado,
  marca,
  tipo_cliente
FROM
  (
    SELECT
      DISTINCT t1.id,
      CAST(t1.data_criacao AS DATE) AS data,
      t1.usuario_criacao AS usuario,
      t1.id_contrato AS contrato,
      t1.primeiro_nivel AS tipo_solicitacao,
      t1.segundo_nivel AS ferramenta_retencao,
      t1.motivo,
      t1.submotivo,
      t5.cidade,
      t5.estado,
      t3.marca,
      CASE
        WHEN t1.tipo_cliente LIKE 'Inad%' THEN 'Inadimplemente'
        WHEN t1.tipo_cliente LIKE 'Adi%' THEN 'Adimplemente'
        ELSE null
      END tipo_cliente
    FROM
      {{ get_catalogo('silver') }}.stage.vw_air_tbl_perfilacao_retencao t1
      LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato t2 
			  ON t1.id_contrato = t2.id_contrato_air
      LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade t3 
			  ON t2.unidade_atendimento = t3.sigla
        AND t3.fonte = 'AIR'
        AND t3.excluido = FALSE
      LEFT JOIN (
        SELECT
          id_contrato,
          MAX(id_endereco) AS id_endereco
        FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega
        WHERE excluido = FALSE
        GROUP BY id_contrato
      ) t4 ON t2.id_contrato = t4.id_contrato
      LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t5 
			  ON t4.id_endereco = t5.id_endereco
        AND t5.fonte = 'AIR'
    WHERE t1.primeiro_nivel = 'Retido'
    UNION ALL
    SELECT
      DISTINCT t1.codigo_contrato_air AS id,
      CAST(t1.data_cancelamento AS DATE) AS data,
      t2.usuario_atribuido AS usuario,
      t1.codigo_contrato_air AS contrato,
      'Cancelado' AS tipo_solicitacao,
      'Cancelado' AS ferramenta_retencao,
      t1.motivo_cancelamento AS motivo,
      t1.sub_motivo_cancelamento AS submotivo,
      t1.cidade,
      t1.estado,
      t1.marca,
      CASE
        WHEN t1.tipo_cancelamento = 'INVOLUNTARIO' AND cancelamento_invol = 'MANUAL' AND motivo_cancelamento = 'CANCELADO POR DEBITO' THEN 'Inadimplemente'
        ELSE 'Adimplemente'
      END tipo_cliente
    FROM
      {{ get_catalogo('gold') }}.base.fato_cancelamento t1
      LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato_retencao t2 
			  ON t1.codigo_contrato_air = t2.id_contrato
        AND t2.status = 'RT_CANCELADO'
    WHERE
      t1.tipo_cancelamento = 'VOLUNTARIO'
      OR (
        t1.tipo_cancelamento = 'INVOLUNTARIO'
        AND cancelamento_invol = 'MANUAL'
        AND motivo_cancelamento = 'CANCELADO POR DEBITO'
      )
  ) a
WHERE a.data >= '2023-06-01'
GROUP BY
  usuario,
  data,
  contrato,
  tipo_solicitacao,
  ferramenta_retencao,
  motivo,
  submotivo,
  cidade,
  estado,
  marca,
  tipo_cliente