WITH cte_notas_fiscais AS (
  SELECT
    id_nota_fiscal,
    ROW_NUMBER() OVER (PARTITION BY id_nota_fiscal ORDER BY data_integracao ASC) AS num
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_notas_fiscais
),
filtered_notas_fiscais AS (
  SELECT id_nota_fiscal
  FROM
    cte_notas_fiscais
  WHERE
    num = 1
)
SELECT
  nf.id_nota_fiscal,
  nf.numero_recibo,
  nf.numero,
  nf.serie,
  nf.codigo_verificacao,
  nf.cliente_nome,
  nf.cliente_documento,
  nf.cliente_id_sydle,
  nf.fatura_codigo,
  nf.fatura_id_sydle,
  nf.status,
  nf.tipo,
  nf.entidade,
  nf.emissor_nome,
  nf.emissor_documento,
  nf.emissor_id_sydle,
  nf.valor_base AS valor_total_nf,
  nf.valor_total_impostos,
  nf.valor_total_impostos_retidos,
  nf.moeda,
  nf.tomador_nome,
  nf.tomador_documento,
  nf.tomador_inscricao_estadual,
  nf.tomador_email,
  nf.tomador_id_sydle,
  nf.tomador_endereco_cep,
  nf.tomador_endereco_logradouro,
  nf.tomador_endereco_numero,
  nf.tomador_endereco_complemento,
  nf.tomador_endereco_referencia,
  nf.tomador_endereco_bairro,
  nf.tomador_endereco_cidade,
  nf.tomador_endereco_codigo_ibge,
  nf.tomador_endereco_estado,
  nf.tomador_endereco_sigla,
  nf.tomador_endereco_pais,
  nf.data_emissao_recibo,
  nf.data_emissao_nota_fiscal,
  nf.lote_nota_fiscal_numero,
  nf.lote_nota_fiscal_id_sydle,
  nf.lote_nota_fiscal_status,
  nf.data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao
FROM
  {{ get_catalogo('silver') }}.stage.vw_sydle_tbl_nota_fiscal AS nf
INNER JOIN
  filtered_notas_fiscais AS fnf
  ON
  nf.id_nota_fiscal = fnf.id_nota_fiscal
