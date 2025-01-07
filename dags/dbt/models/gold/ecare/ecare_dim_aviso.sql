{{
  config(
    alias = 'dim_aviso'
    )
}}

WITH vw_air_tbl_aviso AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_aviso') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  data_inicio,
  data_fim,
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
  aviso_backbone,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM vw_air_tbl_aviso
