{% set catalog_schema_table=source("air_comercial", "tbl_campanha") %}
{% set partition_column="id" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    usuario_criacao,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    usuario_alteracao,
    excluido,
    codigo,
    TRIM(NOME) AS nome,
    recorrencia_desconto,
    recorrencia_vigencia,
    up_operacional_vigencia,
    numero_restricoes_spc,
    valor_restricoes_spc,
    fideliza,
    termo_aceite,
    instalacao_empresa_cnpj,
    instalacao_empresa_nome,
    instalacao_item_codigo,
    instalacao_item_nome,
    instalacao_valor,
    instalacao_desconto_prazo,
    instalacao_desconto_avista,
    instalacao_limite_parcela,
    disponivel_site,
    ativo,
    recorrencia_desconto_data_venda,
    b2b,
    TRY_CAST(data_inicio AS DATE) AS data_inicio,
    TRY_CAST(data_fim AS DATE) AS data_fim,
    pme

  FROM latest
)

SELECT *
FROM transformed
