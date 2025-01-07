{% set catalog_schema_table=source("sydle", "nota_fiscal") %}
{% set partition_column="id_nota_fiscal" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_nota_fiscal,
    numero_recibo,
    numero,
    serie,
    codigo_verificacao,
    cliente_nome,
    cliente_documento,
    cliente_id_sydle,
    fatura_codigo,
    fatura_id_sydle,
    status,
    tipo,
    entidade,
    emissor_nome,
    emissor_documento,
    emissor_id_sydle,
    valor_base,
    valor_total_impostos,
    valor_total_impostos_retidos,
    moeda,
    itens,
    tomador_nome,
    tomador_documento,
    tomador_inscricao_estadual,
    tomador_email,
    tomador_id_sydle,
    tomador_endereco_cep,
    tomador_endereco_logradouro,
    tomador_endereco_numero,
    tomador_endereco_complemento,
    tomador_endereco_referencia,
    tomador_endereco_bairro,
    tomador_endereco_cidade,
    tomador_endereco_codigo_ibge,
    tomador_endereco_estado,
    tomador_endereco_sigla,
    tomador_endereco_pais,
    TRY_CAST(data_emissao_recibo AS TIMESTAMP) AS data_emissao_recibo,
    TRY_CAST(data_emissao_nota_fiscal AS TIMESTAMP) AS data_emissao_nota_fiscal,
    lote_nota_fiscal_numero,
    lote_nota_fiscal_id_sydle,
    lote_nota_fiscal_status,
    TRY_CAST(data_integracao AS TIMESTAMP) AS data_integracao,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

  FROM latest
)

SELECT *
FROM transformed
