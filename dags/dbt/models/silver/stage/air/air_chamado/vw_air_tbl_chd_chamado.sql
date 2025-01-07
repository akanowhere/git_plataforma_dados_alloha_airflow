{% set catalog_schema_table=source("air_chamado", "tbl_chd_chamado") %}
{% set partition_column="id" %}
{% set order_column="data_abertura DESC, data_conclusao DESC, data_transferencia" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    id_classificacao,
    id_fila,
    TRY_CAST(data_abertura AS TIMESTAMP) AS data_abertura,
    TRY_CAST(data_transferencia AS TIMESTAMP) AS data_transferencia,
    TRY_CAST(data_conclusao AS TIMESTAMP) AS data_conclusao,
    usuario_abertura,
    usuario_atribuido,
    motivo_conclusao,
    codigo_contrato,
    codigo_cliente,
    nome_cliente,
    endereco_unidade,
    endereco_bairro,
    endereco_logradouro,
    agendamento_status,
    id_tecnico,
    TRY_CAST(agendamento_data AS DATE) AS agendamento_data,
    agendamento_turno,
    enviado_office_track,
    identificador_chamado,
    id_aviso,
    prioridade,
    TRY_CAST(data_prioridade AS TIMESTAMP) AS data_prioridade,
    contrato_b2b,
    protocolo_primario,
    TRY_CAST(data_confirmacao_agendamento AS TIMESTAMP) AS data_confirmacao_agendamento

  FROM latest
)

SELECT *
FROM transformed
