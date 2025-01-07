SELECT
  t1.id_cliente AS CODIGO_CLIENTE,
  t4.nome NOME_CLIENTE,
  t6.estado AS ESTADO,
  t6.cidade AS CIDADE,
  t6.unidade AS UNIDADE,
  t6.bairro AS BAIRRO,
  t1.id_contrato_air AS CODIGO_CONTRATO,
  t1.pacote_codigo AS PACOTE_CODIGO,
  t1.pacote_nome AS PACOTE_NOME,
  COALESCE(mig.data_venda, t1.data_primeira_ativacao) AS DT_ATIVACAO_PACOTE_ATUAL,
  t1.valor_final AS VALOR_SEM_DESCONTO,
  t1.status AS CONTRATO_STATUS,
  t7.motivo_cancelamento AS TIPO_ENCERRAMENTO,
  t1.data_criacao AS DATA_CRIACAO,
  t1.data_primeira_ativacao AS DATA_ATIVACAO,
  t1.data_cancelamento AS DATA_CANCELAMENTO,
  t1.fechamento_vencimento AS DIA_FECHAMENTO,
  t1.dia_vencimento AS DIA_VENCIMENTO,
  t1.marcador AS MARCADOR_CONTRATO,
  t4.marcador AS MARCADOR_CLIENTE,
  t1.usuario_criacao AS USUARIO_CRIACAO,
  t2.marca AS MARCA,
  t2.cluster AS CLUSTER,
  t4.cupom AS CUPOM,
  t1.flg_fidelizado AS FIDELIZACAO,
  CASE
    WHEN t1.b2b = TRUE THEN 'B2B'
    WHEN t1.pme = TRUE THEN 'PME'
    ELSE 'B2C'
  END AS SEGMENTO,
  t8.data_mudanca_status,
  t8.status_anterior,
  t1.codigo_tipo_cobranca
FROM
  {{ get_catalogo('gold') }}.base.dim_contrato t1
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_unidade t2 
          ON t1.unidade_atendimento = t2.sigla
         AND t2.fonte = 'AIR'
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t4 
         ON t1.id_cliente = t4.id_cliente
        AND t4.fonte = 'AIR'
  LEFT JOIN (
    SELECT
      id_contrato,
      MAX(id_endereco) AS id_endereco
    FROM
      {{ get_catalogo('gold') }}.base.dim_contrato_entrega
    where
      excluido = FALSE
    GROUP BY
      id_contrato
  ) t5 ON t1.id_contrato_air = t5.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t6 
         ON t5.id_endereco = t6.id_endereco
  LEFT JOIN {{ get_catalogo('gold') }}.base.fato_cancelamento t7 
         ON t1.id_contrato_air = t7.CODIGO_CONTRATO_AIR
  LEFT JOIN (
    SELECT
      id_contrato,
      CAST(momento AS DATE) data_mudanca_status,
      CASE
        WHEN tipo LIKE '%CANCELADO%' THEN 'ST_CONT_CANCELADO'
        WHEN tipo LIKE '%HABILITADO%' THEN 'ST_CONT_HABILITADO'
        WHEN tipo LIKE '%SUSPENSO%' AND observacao LIKE '%suspenso por débito%' THEN 'ST_CONT_SUSP_DEBITO'
        WHEN tipo LIKE '%SUSPENSO%' AND observacao LIKE '%falta de pagamento%' THEN 'ST_CONT_SUSP_DEBITO'
        WHEN tipo LIKE '%SUSPENSO%' AND observacao LIKE '%Solicitação de suspensão%' THEN 'ST_CONT_SUSP_SOLICITACAO'
      END status_anterior,
      ROW_NUMBER() OVER(
        PARTITION BY id_contrato
        ORDER BY
          momento DESC
      ) linha
    FROM
      {{ get_catalogo('gold') }}.base.dim_cliente_evento
    WHERE
      tipo IN (
        'EVT_CONTRATO_CANCELADO EVT_CONTRATO_CANCELADO',
        'EVT_CONTRATO_CANCELADO',
        'EVT_CONTRATO_HABILITADO_APOS_PAGAMENTO',
        'EVT_CONTRATO_HABILITADO_AUTOMATICAMENTE',
        'EVT_CONTRATO_HABILITADO_CONFIANCA',
        'EVT_CONTRATO_HABILITADO_ENVIO_COMPROVANTE',
        'EVT_CONTRATO_HABILITADO_SOLICITACAO',
        'EVT_CONTRATO_SUSPENSO'
      )
  ) t8 ON t1.id_contrato_air = t8.id_contrato
      AND t8.linha = 2
  LEFT JOIN (
    SELECT
      cm.id_contrato,
      cm.novo_versao,
      v.natureza,
      v.equipe,
      v.data_venda
    FROM
      {{ get_catalogo('gold') }}.base.dim_migracao cm
      LEFT JOIN {{ get_catalogo('gold') }}.venda.dim_venda v on cm.id_venda = v.id_venda
      and fonte = 'AIR'
    WHERE
      cm.reprovada = FALSE
      AND cm.id = (
        SELECT
          MAX(id)
        FROM
          {{ get_catalogo('gold') }}.base.dim_migracao
        WHERE
          reprovada = FALSE
          and id_contrato = cm.id_contrato
      )
  ) mig ON mig.id_contrato = t1.id_contrato
  AND mig.novo_versao = t1.versao_contrato_air