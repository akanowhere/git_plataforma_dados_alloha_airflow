{% set catalog_schema_table=get_catalogo("bronze") ~ ".legado.vendas" %}
{% set partition_column="id_venda, fonte" %}
{% set order_column="versao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
)

SELECT
  CAST(afiliado_assine AS bigint)
  ,CAST(bairro AS string)
  ,CAST(campanha_codigo AS string)
  ,CAST(campanha_nome AS string)
  ,CAST(canal AS string)
  ,CAST(canal_tratado AS string)
  ,CAST(cep AS string)
  ,CAST(cidade AS string)
  ,CAST(cobranca AS string)
  ,CAST(cod_tipo_cobranca AS string)
  ,CAST(concretizada AS string)
  ,CAST(confirmada AS string)
  ,CAST(consultor_autonomo AS bigint)
  ,CAST(cpf_cnpj AS string)
  ,CAST(cupom_valido AS bigint)
  ,CAST(data_alteracao AS STRING)
  ,CAST(data_criacao AS STRING)
  ,CAST(data_extracao AS STRING)
  ,CAST(REPLACE(LEFT(data_venda, 10), '/', '-') AS date) AS data_venda
  ,CAST(desconto AS string)
  ,CAST(duracao_desconto AS bigint)
  ,CAST(endereco_cobranca AS string)
  ,CAST(endereco_entrega AS string)
  ,CAST(equipe AS string)
  ,CAST(estado AS string)
  ,CAST(excluido AS string)
  ,CAST(fase_atual AS string)
  ,CAST(fonte AS string)
  ,CAST(id_campanha AS string)
  ,CAST(id_cliente AS string)
  ,CAST(id_contrato AS string)
  ,CAST(id_contrato_air AS bigint)
  ,CAST(id_contrato_ixc AS bigint)
  ,CAST(id_endereco_cobranca AS bigint)
  ,CAST(id_endereco_entrega AS bigint)
  ,CAST(id_lead AS string)
  ,CAST(id_processo_venda_sydle AS string)
  ,CAST(id_regra_suspensao AS string)
  ,CAST(id_vencimento AS string)
  ,CAST(id_venda AS string)
  ,CAST(id_vendedor AS string)
  ,CAST(latitude_cobranca AS string)
  ,CAST(latitude_entrega AS string)
  ,CAST(legado_id AS bigint)
  ,CAST(legado_sistema AS string)
  ,CAST(longitude_cobranca AS string)
  ,CAST(longitude_entrega AS string)
  ,CAST(macro_regional AS string)
  ,CAST(marca AS string)
  ,CAST(natureza AS string)
  ,CAST(nome_regional AS string)
  ,CAST(numero_cobranca AS string)
  ,CAST(numero_entrega AS string)
  ,CAST(pacote_base_codigo AS string)
  ,CAST(pacote_base_nome AS string)
  ,CAST(pacote_tv AS string)
  ,CAST(pacote_valor_total AS string)
  ,CAST(possui_internet AS string)
  ,CAST(possui_telefone AS string)
  ,CAST(possui_tv AS string)
  ,CAST(produto AS string)
  ,CAST(recorrencia_meses_desconto AS string)
  ,CAST(recorrencia_percentual_desconto AS string)
  ,CAST(regional AS string)
  ,CAST(segmento AS string)
  ,CAST(sk_venda AS bigint)
  ,CAST(tipo_canal AS string)
  ,CAST(tipo_cliente AS string)
  ,CAST(tipo_residencia AS string)
  ,CAST(unidade AS string)
  ,CAST(unidade_atendimento AS string)
  ,CAST(usuario_alteracao AS string)
  ,CAST(usuario_criacao AS string)
  ,CAST(valor_final_com_desconto AS string)
  ,CAST(valor_referencia_b2b AS string)
  ,CAST(valor_total AS string)
  ,CAST(velocidade AS string)
  ,CAST(versao AS bigint)
  ,CAST(_metadata AS string)
FROM
  latest
WHERE
  fonte <> 'AIR'
