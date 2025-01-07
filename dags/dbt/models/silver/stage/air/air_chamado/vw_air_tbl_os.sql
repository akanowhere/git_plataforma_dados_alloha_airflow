{% set catalog_schema_table=source("air_chamado", "tbl_os") %}
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
    id_agenda,
    id_chamado,
    id_terceirizada,
    TRY_CAST(agendamento_data AS TIMESTAMP) AS agendamento_data,
    turno,
    equipe,
    classificacao,
    servico,
    id_tecnico,
    solicitacao_atendida,
    codigo_cliente,
    TRIM(cliente_nome) AS cliente_nome,
    cpfcnpj,
    contrato_codigo,
    pacote,
    telefone,
    email,
    endereco,
    bairro,
    logradouro,
    lat,
    lng,
    endereco_referencia,
    unidade,
    codigo_fila,
    observacao,
    status,
    motivo_conclusao,
    TRY_CAST(data_conclusao AS TIMESTAMP) AS data_conclusao,
    integracao_status,
    integracao_mensagem,
    integracao_codigo,
    origem_agendamento,
    TRY_CAST(data_confirmacao_agendamento AS TIMESTAMP) AS data_confirmacao_agendamento,
    origem_confirmacao_agendamento,
    TRY_CAST(data_reagendamento AS TIMESTAMP) AS data_reagendamento,
    origem_reagendamento,
    pme,
    ftta,
    id_reserva_officetrack,
    telefone_adicional,
    contatos,
    cep,
    endereco_complemento,
    endereco_numero,
    cidade,
    estado,
    area_distribuicao,
    codigo_tipo_tarefa,
    tipo_servico,
    pontos_tv,
    b2b,
    status_interno,
    existia_data_anterior,
    motivo_revisao,
    local_instalacao,
    pesquisa_tecnologia_anterior,
    whatsapp_capturado,
    controle_envio_whatsapp,
    protocolo_secundario,
    chave_wk,
    bucket,
    TRY_CAST(primeira_data_ag AS TIMESTAMP) AS primeira_data_ag,
    conveniencia_cliente,
    link_wmt,
    quant_visitas,
    priorizar_roteirizacao,
    id_hierarquia_fechamento,
    confirma_endereco,
    CASE
    WHEN (posicao_sap IS NULL OR trim(posicao_sap) = '' ) THEN NULL
    ELSE posicao_sap end as posicao_sap

  FROM latest
)

SELECT *
FROM transformed
