WITH bronze_legado__faturas_mailing_deletadas AS (
  SELECT *
  FROM {{ source('legado', 'faturas_mailing_deletadas') }}
),

silver_stage__vw_sydle_tbl_fatura AS (
  SELECT *
  FROM {{ ref('vw_sydle_tbl_fatura') }}
),

silver_stage__vw_maling_tbl_fatura AS (
  SELECT *
  FROM {{ ref('vw_mailing_tbl_fatura') }}
),

gold_base__dim_cliente AS (
  SELECT *
  FROM {{ ref('dim_cliente') }}
),

gold_base__dim_contrato AS (
  SELECT *
  FROM {{ ref('dim_contrato') }}
),

gold_base__dim_unidade AS (
  SELECT *
  FROM {{ ref('dim_unidade') }}
),

filtered_air_customers AS (
  SELECT DISTINCT
    id_cliente,
    data_criacao,
    excluido,
    CASE
      WHEN cnpj IS NULL THEN REPLACE(REPLACE(cpf, '.', ''), '-', '')
      ELSE REPLACE(REPLACE(REPLACE(cnpj, '.', ''), '-', ''), '/', '')
    END AS cpf_cnpj

  FROM gold_base__dim_cliente

  WHERE fonte = 'AIR'
),

filtered_air_contracts AS (
  SELECT DISTINCT
    ctr.id_contrato,
    ctr.id_cliente,
    ctr.unidade_atendimento,
    uni.marca

  FROM gold_base__dim_contrato AS ctr
  LEFT JOIN gold_base__dim_unidade AS uni ON ctr.unidade_atendimento = uni.sigla
),

filtered_sydle_invoices_excluding_deleted_mailing AS (
  SELECT
    id_fatura,
    id_cliente AS id_cliente_sydle,
    codigo AS codigo_fatura_sydle,
    CASE
      WHEN LOWER(codigo_externo) LIKE '%[a-z]%' THEN NULL
      ELSE REPLACE(REPLACE(codigo_externo, CHAR(13), ''), CHAR(10), '')
    END AS contrato_air,
    cliente,
    CASE
      WHEN LEN(tipo_documento_cliente) <= 4 THEN tipo_documento_cliente
      ELSE NULL
    END AS tipo_documento,
    CASE
      WHEN LEN(documento_cliente) <= 18 THEN documento_cliente
      ELSE NULL
    END AS cpf_cnpj,
    mes_referencia,
    data_criacao,
    data_vencimento,
    data_pagamento,
    data_atualizacao,
    classificacao,
    forma_pagamento,
    beneficiario,
    statusfatura AS status_fatura,
    valor_total AS valor_fatura,
    valor_sem_multa_juros,
    valor_pago,
    data_extracao,
    id_legado,
    data_extracao_db,
    responsavel_registro_pagamento_manual_nome,
    responsavel_registro_pagamento_manual_login

  FROM silver_stage__vw_sydle_tbl_fatura

  WHERE codigo NOT IN (
      SELECT codigo_fatura_sydle
      FROM bronze_legado__faturas_mailing_deletadas
    )
),

filtered_sydle_invoices_excluding_deleted_mailing__join__filtered_air_customers AS (
  SELECT DISTINCT
    faturas.id_fatura,
    faturas.id_cliente_sydle,
    faturas.codigo_fatura_sydle,
    clientes.id_cliente AS id_cliente_air,
    clientes.data_criacao AS cliente_data_criacao,
    clientes.excluido,
    faturas.contrato_air,
    faturas.cliente,
    faturas.tipo_documento,
    faturas.cpf_cnpj,
    faturas.mes_referencia,
    faturas.data_criacao,
    faturas.data_vencimento,
    faturas.data_pagamento,
    faturas.data_atualizacao,
    faturas.classificacao,
    faturas.forma_pagamento,
    faturas.beneficiario,
    faturas.status_fatura,
    faturas.valor_fatura,
    faturas.valor_sem_multa_juros,
    faturas.valor_pago,
    faturas.data_extracao,
    faturas.id_legado,
    faturas.data_extracao_db,
    faturas.responsavel_registro_pagamento_manual_nome,
    faturas.responsavel_registro_pagamento_manual_login

  FROM filtered_sydle_invoices_excluding_deleted_mailing AS faturas
  LEFT JOIN filtered_air_customers AS clientes ON faturas.cpf_cnpj = clientes.cpf_cnpj
),

add_brand_to_invoices AS (
  SELECT DISTINCT
    faturas.id_fatura,
    faturas.id_cliente_sydle,
    faturas.codigo_fatura_sydle,
    faturas.id_cliente_air,
    faturas.cliente_data_criacao,
    faturas.excluido,
    faturas.contrato_air,
    faturas.cliente,
    faturas.tipo_documento,
    faturas.cpf_cnpj,
    faturas.mes_referencia,
    faturas.data_criacao,
    faturas.data_vencimento,
    faturas.data_pagamento,
    faturas.data_atualizacao,
    faturas.classificacao,
    faturas.forma_pagamento,
    faturas.beneficiario,
    faturas.status_fatura,
    faturas.valor_fatura,
    faturas.valor_sem_multa_juros,
    faturas.valor_pago,
    faturas.data_extracao,
    faturas.id_legado,
    contratos.unidade_atendimento,
    contratos.marca,
    faturas.data_extracao_db,
    faturas.responsavel_registro_pagamento_manual_nome,
    faturas.responsavel_registro_pagamento_manual_login,
    CASE
      WHEN contratos.marca IN ('CLICK', 'UNIVOX')
        AND faturas.id_legado IS NOT NULL
        AND vw_mailing_fatura.codigo_fatura IS NULL
        THEN '0'
      ELSE '1'
    END AS fat_cobranca

  FROM filtered_sydle_invoices_excluding_deleted_mailing__join__filtered_air_customers AS faturas
  LEFT JOIN filtered_air_contracts AS contratos
    ON faturas.id_cliente_air = contratos.id_cliente
    AND faturas.contrato_air = contratos.id_contrato
  LEFT JOIN (
    SELECT DISTINCT
      codigo_fatura,
      marca
    FROM silver_stage__vw_maling_tbl_fatura
    WHERE marca IN ('CLICK', 'UNIVOX')
  ) AS vw_mailing_fatura
    ON contratos.marca = vw_mailing_fatura.marca
    AND contratos.marca IN ('CLICK', 'UNIVOX')
    AND vw_mailing_fatura.codigo_fatura = SUBSTRING(faturas.id_legado, (CHARINDEX('_', faturas.id_legado) + 1), LEN(faturas.id_legado))
),

final AS (
  SELECT
    id_fatura,
    id_cliente_sydle,
    codigo_fatura_sydle,
    id_cliente_air,
    cliente_data_criacao,
    excluido,
    contrato_air,
    cliente,
    tipo_documento,
    cpf_cnpj,
    mes_referencia,
    data_criacao,
    data_vencimento,
    data_pagamento,
    data_atualizacao,
    UPPER({{ translate_column('classificacao') }}) AS classificacao,
    forma_pagamento,
    beneficiario,
    UPPER({{ translate_column('status_fatura') }}) AS status_fatura,
    valor_fatura,
    valor_sem_multa_juros,
    valor_pago,
    id_legado AS id_legado_fatura,
    unidade_atendimento,
    marca,
    data_extracao_db,
    fat_cobranca,
    responsavel_registro_pagamento_manual_nome,
    responsavel_registro_pagamento_manual_login,
    data_extracao,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP) AS data_referencia

  FROM add_brand_to_invoices

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_fatura
    ORDER BY
      CASE
        WHEN cliente_data_criacao <= data_criacao THEN 1
        ELSE 2
      END,
      cliente_data_criacao DESC,
      excluido ASC
  ) = 1
)

SELECT *
FROM final
