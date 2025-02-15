SELECT
  TRY_CAST(id_contrato AS BIGINT),
  TRY_CAST(data_cancelamento AS DATE),
  TRY_CAST(data_ativacao AS DATE),
  TRY_CAST(motivo_cancelamento AS STRING),
  TRY_CAST(cidade AS STRING),
  TRY_CAST(regiao AS STRING),
  TRY_CAST(estado AS STRING),
  TRY_CAST(canal AS STRING),
  TRY_CAST(marca AS STRING),
  TRY_CAST(data_extracao AS TIMESTAMP),
  TRY_CAST(unidade AS STRING),
  TRY_CAST(regional AS STRING),
  TRY_CAST(produto AS STRING),
  TRY_CAST(pacote_tv AS STRING),
  TRY_CAST(tipo_tv AS STRING),
  TRY_CAST(dia_fechamento AS BIGINT),
  TRY_CAST(dia_vencimento AS BIGINT),
  TRY_CAST(aging AS BIGINT),
  TRY_CAST(segmento AS STRING),
  TRY_CAST(equipe AS STRING),
  TRY_CAST(tipo_cancelamento AS STRING),
  TRY_CAST(sub_motivo_cancelamento AS STRING),
  TRY_CAST(cancelamento_invol AS STRING),
  TRY_CAST(pacote_nome AS STRING),
  TRY_CAST(valor_final AS DOUBLE),
  TRY_CAST(cluster AS STRING),
  TRY_CAST(fonte AS STRING),
  TRY_CAST(legado_id AS BIGINT),
  TRY_CAST(legado_sistema AS STRING),
  TRY_CAST(canal_tratado AS STRING),
  TRY_CAST(tipo_canal AS STRING),
  TRY_CAST(id_contrato_air AS BIGINT),
  TRY_CAST(id_contrato_ixc AS BIGINT),
  TRY_CAST(flg_fidelizado AS STRING),
  TRY_CAST(macro_regional AS STRING),
  TRY_CAST(nome_regional AS STRING)
FROM {{ get_catalogo('bronze') }}.legado.cancelamento
