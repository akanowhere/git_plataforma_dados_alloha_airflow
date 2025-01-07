{% set catalog_schema_table=source("air_internet", "tbl_aviso") %}
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
    TRY_CAST(data_inicio AS TIMESTAMP) AS data_inicio,
    TRY_CAST(data_fim AS TIMESTAMP) AS data_fim,
    nivel_criticidade,
    protocolo,
    ocorrencia,
    afeta_internet,
    afeta_telefonia,
    afeta_tv,
    impacto,
    equipe_responsavel,
    proxima_atualizacao,
    vigente,
    ura_ativa,
    tempo_solucao,
    falha_energia,
    falha_sabotagem,
    sobrescrever_aviso_onu,
    giga_mais,
    clientes_afetados,
    id_problema,
    estado,
    cidade,
    regional,
    descricao,
    id_os_rede_externa,
    manutencao_programada,
    aviso_backbone

  FROM latest
)

SELECT *
FROM transformed
