WITH dim_contrato AS (
  SELECT * FROM {{ ref('dim_contrato') }}
),

dim_cliente AS (
  SELECT * FROM {{ ref('dim_cliente') }}
),

dim_endereco AS (
  SELECT * FROM {{ ref('dim_endereco') }}
),

fato_chamadas_recebidas_voz AS (
  SELECT * FROM {{ ref('fato_chamadas_recebidas_voz') }}
),

filtra_primeira_ocorrencia_e_campanhas_especificas AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_chamada ORDER BY data_inicio_chamada ASC) AS rn
  FROM fato_chamadas_recebidas_voz
  WHERE campanha IN ('NIU_PRINCIPAL', 'LIGUE_PRINCIPAL', 'PRINCIPAL MOB B2C', 'PRINCIPAL', 'Campanha Principal Sumicity B2C')
  QUALIFY rn = 1
),

base_chamadas_recebidas_voz AS (
  SELECT
    id_chamada,
    CAST(data_inicio_chamada AS DATE) AS data_chamada,
    data_inicio_chamada,
    campanha,
    agente,
    competencia,
    'Voz' AS tipo_midia,
    cpf_cnpj,
    contrato AS id_contrato,
    CASE
      WHEN agente IS NOT NULL THEN 1
      WHEN abandonada > 0 THEN 2
      WHEN agente IS NULL AND (motivo_retencao_ura IS NOT NULL AND motivo_retencao_ura <> 'Derivado' AND motivo_retencao_ura <> 'Desistência') THEN 0
      WHEN motivo_retencao_ura IS NULL OR motivo_retencao_ura = 'Desistência' THEN 3
      WHEN agente IS NULL AND motivo_retencao_ura = 'Derivado' THEN 4
    END AS id_condicao_desligamento,
    CASE
      WHEN agente IS NOT NULL THEN 'Derivada Humano'
      WHEN abandonada > 0 THEN 'Abandonada'
      WHEN agente IS NULL AND (motivo_retencao_ura IS NOT NULL AND motivo_retencao_ura <> 'Derivado' AND motivo_retencao_ura <> 'Desistência') THEN 'Retencao'
      WHEN motivo_retencao_ura IS NULL OR motivo_retencao_ura = 'Desistência' THEN 'Desistencia'
      WHEN agente IS NULL AND motivo_retencao_ura = 'Derivado' THEN 'Fora do Horario'
    END AS condicao_desligamento,
    motivo_retencao_ura,
    CASE
      WHEN tempo_ivr IS NULL OR tempo_ivr NOT LIKE '%:%:%' THEN NULL
      ELSE
        CAST(SUBSTRING(tempo_ivr, 1, 2) AS INTEGER) * 3600 +  -- horas para segundos
        CAST(SUBSTRING(tempo_ivr, 4, 2) AS INTEGER) * 60 +   -- minutos para segundos
        CAST(SUBSTRING(tempo_ivr, 7, 2) AS INTEGER)          -- segundos
    END AS tempo_ivr_seg,
    marca,
    'Five9' AS fonte,
    posicao

  FROM filtra_primeira_ocorrencia_e_campanhas_especificas
),

indicadores_iniciais AS (
  SELECT
    *,
    CASE WHEN id_chamada IS NOT NULL THEN 1 ELSE 0 END AS flg_cham_recebidas,
    CASE WHEN id_condicao_desligamento = 1 THEN 1 ELSE 0 END AS flg_cham_derivadas_humano,
    CASE WHEN id_condicao_desligamento = 2 THEN 1 ELSE 0 END AS flg_cham_abandonadas,
    CASE WHEN id_condicao_desligamento = 3 THEN 1 ELSE 0 END AS flg_cham_desistencia,
    CASE WHEN id_condicao_desligamento = 4 THEN 1 ELSE 0 END AS flg_cham_fora_horario,
    CASE WHEN id_condicao_desligamento = 0 THEN 1 ELSE 0 END AS flg_cham_retidas,
    CASE
      WHEN id_condicao_desligamento = 0 AND
        (
          motivo_retencao_ura NOT LIKE '%-APP' AND
          motivo_retencao_ura NOT LIKE '%-AP' AND
          motivo_retencao_ura NOT LIKE '%-WPP' AND
          motivo_retencao_ura NOT LIKE '%-WP' AND
          motivo_retencao_ura NOT IN ('MP-F-FNL-AVE', 'MP-F-EA', 'SP-SPD-OP1-HC-ND-EC')
        ) THEN 1
      ELSE 0
    END AS flg_cham_retidas_autosservicos,
    CASE
      WHEN id_condicao_desligamento = 0 AND
        (
          motivo_retencao_ura LIKE '%-APP' AND
          motivo_retencao_ura LIKE '%-AP' AND
          motivo_retencao_ura LIKE '%-WPP' AND
          motivo_retencao_ura LIKE '%-WP' AND
          motivo_retencao_ura IN ('MP-F-FNL-AVE', 'MP-F-EA', 'SP-SPD-OP1-HC-ND-EC')
        ) THEN 1
      ELSE 0
    END AS flg_cham_retidas_engajamento_app_wpp

  FROM base_chamadas_recebidas_voz
),

indicadores_repetidas AS (
  SELECT
    id_chamada,
    CASE
      WHEN cpf_cnpj = LEAD(cpf_cnpj, 1, '-') OVER (ORDER BY cpf_cnpj, data_inicio_chamada) AND
        LEAD(data_inicio_chamada, 1) OVER (ORDER BY cpf_cnpj, data_inicio_chamada) <= DATEADD(DAY, 1, data_inicio_chamada) THEN 1
      ELSE 0
    END AS cham_repetidas_24h,
    CASE
      WHEN cpf_cnpj = LEAD(cpf_cnpj, 1, '-') OVER (ORDER BY cpf_cnpj, data_inicio_chamada) AND
        LEAD(data_inicio_chamada, 1) OVER (ORDER BY cpf_cnpj, data_inicio_chamada) < DATEADD(DAY, 1, CAST(data_inicio_chamada AS DATE)) THEN 1
      ELSE 0
    END AS cham_repetidas_dia_corrente

  FROM base_chamadas_recebidas_voz
  WHERE cpf_cnpj IS NOT NULL
),

unifica_indicadores AS (
  SELECT
    indicadores_iniciais.*,
    CASE WHEN indicadores_repetidas.cham_repetidas_24h IS NULL THEN 0 ELSE indicadores_repetidas.cham_repetidas_24h END AS flg_cham_repetidas_24h,
    CASE WHEN indicadores_repetidas.cham_repetidas_dia_corrente IS NULL THEN 0 ELSE indicadores_repetidas.cham_repetidas_dia_corrente END AS flg_cham_repetidas_dia_corrente

  FROM indicadores_iniciais
  LEFT JOIN indicadores_repetidas ON indicadores_iniciais.id_chamada = indicadores_repetidas.id_chamada
),

base_clientes_air AS (
  SELECT
    dim_contrato.id_contrato,
    dim_cliente.id_cliente,
    CASE
      WHEN dim_cliente.cpf IS NULL THEN REPLACE(REPLACE(REPLACE(dim_cliente.cnpj, '.', ''), '-', ''), '/', '')
      ELSE REPLACE(REPLACE(dim_cliente.cpf, '.', ''), '-', '')
    END AS cpf_cnpj,
    dim_endereco.cidade,
    dim_contrato.unidade_atendimento

  FROM dim_contrato
  INNER JOIN dim_cliente ON dim_contrato.id_cliente = dim_cliente.id_cliente
  LEFT JOIN dim_endereco ON dim_contrato.id_endereco_cobranca = dim_endereco.id_endereco
),

juncao_bases_indicadores_e_clientes_air AS (
  SELECT
    unifica_indicadores.*,
    base_clientes_air.id_cliente,
    base_clientes_air.cidade,
    base_clientes_air.unidade_atendimento

  FROM unifica_indicadores
  LEFT JOIN base_clientes_air ON unifica_indicadores.cpf_cnpj = base_clientes_air.cpf_cnpj AND unifica_indicadores.id_contrato = base_clientes_air.id_contrato
),

final AS (
  SELECT
    id_chamada,
    data_chamada,
    campanha,
    agente,
    competencia,
    posicao,
    tipo_midia,
    cpf_cnpj,
    id_cliente,
    id_contrato,
    cidade,
    unidade_atendimento,
    id_condicao_desligamento,
    condicao_desligamento,
    motivo_retencao_ura,
    flg_cham_recebidas,
    flg_cham_derivadas_humano,
    flg_cham_abandonadas,
    flg_cham_desistencia,
    flg_cham_fora_horario,
    flg_cham_retidas,
    flg_cham_retidas_autosservicos,
    flg_cham_retidas_engajamento_app_wpp,
    flg_cham_repetidas_24h,
    flg_cham_repetidas_dia_corrente,
    tempo_ivr_seg,
    marca,
    fonte

  FROM juncao_bases_indicadores_e_clientes_air
)

SELECT *
FROM
  final
