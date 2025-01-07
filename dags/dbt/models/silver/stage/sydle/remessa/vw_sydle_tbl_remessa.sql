{% set catalog_schema_table=source("sydle", "remessa") %}
{% set partition_column="id_remessa" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_remessa,
    pagador_nome,
    pagador_documento,
    pagador_id_sydle,
    beneficiario_nome,
    beneficiario_documento,
    beneficiario_id_sydle,
    status_remessa,
    codigo_remessa,
    valor_remessa,
    valor_pago,
    valor_estornado,
    convenio_nome,
    convenio_identificador,
    convenio_ativo,
    convenio_emissor_nome,
    convenio_emissor_documento,
    convenio_emissor_id_sydle,
    cliente_nome,
    cliente_documento,
    cliente_id_sydle,
    grupo_de_forma_de_pagamento,
    condicao_de_pagamento_nome,
    condicao_de_pagamento_identificador,
    condicao_de_pagamento_parcela,
    TRY_CAST(data_vencimento AS TIMESTAMP) AS data_vencimento,
    multa,
    juros,
    TRY_CAST(data_status AS TIMESTAMP) AS data_status,
    TRY_CAST(data_previsao_repasse AS TIMESTAMP) AS data_previsao_repasse,
    TRY_CAST(data_repasse AS TIMESTAMP) AS data_repasse,
    TRY_CAST(data_contestacao AS TIMESTAMP) AS data_contestacao,
    valor_taxas,
    valor_liquido_repasse,
    retorno,
    detalhes_retorno,
    faturas,
    parcelas,
    TRY_CAST(data_integracao AS TIMESTAMP) AS data_integracao,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

  FROM latest
)

SELECT *
FROM transformed
