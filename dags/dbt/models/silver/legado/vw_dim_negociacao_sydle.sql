SELECT
  CAST(id_negociacao AS string)
  ,CAST(codigo AS string)
  ,CAST(fatura_original AS string)
  ,CAST(id_negociador AS string)
  ,lcase(CAST(nm_negociador AS string)) as nm_negociador
  ,CAST(data_criacao AS DATE)
  ,CAST(fonte_negociacao AS string)
FROM {{ get_catalogo('bronze') }}.legado.negociacao_sydle;