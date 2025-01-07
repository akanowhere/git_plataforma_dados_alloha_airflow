{% set catalog_schema_table=source("air_comercial", "tbl_lead") %}
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
    tipo,
    id_cliente,
    nome,
    telefone,
    email,
    email_status,
    end_id_cidade,
    end_cidade_nome,
    end_unidade,
    end_cod_ibge,
    end_bairro_nome,
    end_tipo_logradouro,
    end_logradouro_nome,
    end_cep,
    end_referencia,
    end_complemento,
    end_numero,
    end_latitude,
    end_longitude,
    cpf_cnpj,
    pf_data_nascimento,
    pf_tratamento,
    pf_estado_civil,
    pf_rg_numero,
    pf_rg_orgao,
    pj_nome_fantasia,
    pj_inscricao_estadual,
    pj_inscricao_municipal,
    pj_atividade_principal,
    legado_id,
    legado_sistema,
    fonte,
    grupo,
    telefone_adicional,
    email_adicional,
    email_adicional_status,
    senha_sac,
    qualificacao,
    pf_nome_pai,
    pf_nome_mae,
    pf_sexo,
    credito_autorizado,
    viabilidade_autorizada,
    viabilidade_usuario,
    TRY_CAST(viabilidade_data AS TIMESTAMP) AS viabilidade_data,
    viabilidade_justificativa,
    tipo_residencia,
    id_condominio,
    bloco,
    apartamento,
    end_certificado,
    end_verificado,
    end_completo,
    uuid_lead_rd_station

  FROM latest
)

SELECT *
FROM transformed
