WITH velocidade_contrato AS (
  SELECT id_contrato, versao_contrato, velocidade
  FROM (
  SELECT A.id_contrato, A.versao_contrato, trim(banda.descricao) as velocidade, A.data_alteracao,
  ROW_NUMBER() OVER(PARTITION BY A.id_contrato, A.versao_contrato ORDER BY A.data_alteracao DESC) AS rn
  FROM {{ ref('vw_air_tbl_contrato_produto') }} AS A
  LEFT JOIN (
      SELECT sap_produto_codigo, id_banda
      FROM {{ ref('vw_air_tbl_produto') }}
      WHERE excluido = 0
  ) AS produto ON A.item_codigo = produto.sap_produto_codigo
  LEFT JOIN (
      SELECT id, descricao
      FROM {{ ref('vw_air_tbl_banda') }}
      WHERE excluido = 0
  ) AS banda ON produto.id_banda = banda.id
  WHERE banda.descricao IS NOT NULL
  AND (UPPER(A.item_nome) LIKE '%SCM%' OR UPPER(A.item_nome) LIKE '%INTERNET%')
  and A.excluido = FALSE
  )
  where rn = 1
)

SELECT DISTINCT
  DATE_FORMAT(tvendas.data_criacao, 'yyyy-MM-dd HH:mm') AS data_criacao,
  tvendas.usuario_criacao,
  DATE_FORMAT(tvendas.data_alteracao, 'yyyy-MM-dd HH:mm') AS data_alteracao,
  tvendas.usuario_alteracao,
  tvendas.excluido,
  tvendas.natureza,
  tvendas.fase_atual,
  tvendas.id_vendedor,
  tvendas.id_campanha,
  tvendas.id_lead,
  tvendas.id_contrato,
  tcontrato.legado_id,
  tcontrato.legado_sistema,
  tvendas.id_regra_suspensao,
  tvendas.id_vencimento,
  tvendas.id_endereco AS id_endereco_entrega,
  tcontrato.id_endereco_cobranca,
  tvendas.unidade_atendimento,
  tvendas.cod_tipo_cobranca,
  DATE_FORMAT(tvendas.data_venda, 'yyyy-MM-dd HH:mm') AS data_venda,
  tvendas.pacote_base_nome,
  tvendas.pacote_base_codigo,
  tvendas.pacote_valor_total,
  tvendas.concretizada,
  tvendas.confirmada,
  tvendas.valor_total,
  tvendas.possui_internet,
  tvendas.possui_tv,
  tvendas.possui_telefone,
  tvendas.recorrencia_percentual_desconto,
  tvendas.recorrencia_meses_desconto,
  tvendas.equipe,
  tvendas.id_processo_venda_sydle,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
  tcampanha.codigo AS campanha_codigo,
  tcampanha.nome AS campanha_nome,
  tvendas.id AS id_venda,
  tcontrato.id_cliente,
  CASE
    WHEN UPPER(tcontrato.legado_sistema) = 'URBE' THEN 'URBE'
    WHEN tcontrato.b2b = 'true' THEN 'B2B'
    WHEN tcontrato.pme = 'true' THEN 'PME'
    ELSE 'B2C'
  END AS segmento,
  tunidade.regional,
  tcliente.tipo AS tipo_cliente,
  ROUND(
    tvendas.valor_total - (
      COALESCE(
        tvendas.pacote_valor_total * (
          COALESCE(
            tvendas.recorrencia_percentual_desconto,
            tdesconto.desconto
          )
        ),
        0
      ) / 100
    ),
    2
  ) AS valor_final_com_desconto,
  COALESCE(
    tvendas.recorrencia_percentual_desconto,
    tdesconto.desconto
  ) AS desconto,
  'AIR' AS fonte,
  UPPER(tunidade.marca) AS marca,
  CAST(NULL AS STRING) AS endereco_cobranca,
  CAST(NULL AS STRING) AS numero_cobranca,
  CAST(NULL AS STRING) AS latitude_cobranca,
  CAST(NULL AS STRING) AS longitude_cobranca,
  tendereco_entrega.logradouro AS endereco_entrega,
  tendereco_entrega.numero AS numero_entrega,
  tendereco_entrega.latitude AS latitude_entrega,
  tendereco_entrega.longitude AS longitude_entrega,
  tendereco_entrega.bairro AS bairro_entrega,
  tendereco_entrega.cep AS cep_entrega,
  tendereco_cidade.nome AS cidade,
  tendereco_cidade.estado,
  CASE
    WHEN tvendas.equipe IN ('EQUIPE_PAP', 'EQUIPE_PAP_BRASILIA') THEN 'PAP DIRETO'
    WHEN tvendas.equipe = 'EQUIPE_TLV_ATIVO' THEN 'ATIVO'
    WHEN tvendas.equipe IN (
        'EQUIPE_AVANT_TELECOM_EIRELI',
        'EQUIPE_BLISS_TELECOM',
        'EQUIPE_FIBRA_TELECOM',
        'EQUIPE_RR_TELECOM',
        'EQUIPE_MAKOTO_TELECOM',
        'EQUIPE_MORANDI',
        'EQUIPE_LITORAL_SAT'
      ) THEN 'PAP INDIRETO'
    WHEN tvendas.equipe LIKE '%PAP%' THEN 'PAP INDIRETO'
    WHEN tvendas.equipe IN ('AGENTE_DIGITAL_DIRETO', 'DIGITAL_ALLOHA') THEN 'CHATBOT'
    WHEN tvendas.equipe LIKE '%AGENTE_DIGITAL%' THEN 'AGENTE DIGITAL'
    WHEN tvendas.equipe LIKE '%DIGITAL_ASSINE%' THEN 'ASSINE'
    WHEN tvendas.equipe LIKE 'EQUIPE_DIGITAL'
      OR tvendas.equipe LIKE 'EQUIPE_DIGITAL_BRA%'
      OR tvendas.equipe LIKE '%COM_DIGITAL%'
      OR tvendas.equipe LIKE 'DIGITAL' THEN 'DIGITAL'
    WHEN tvendas.equipe LIKE '%LOJA%' THEN 'LOJA'
    WHEN tvendas.equipe LIKE '%OUTBOUND%' THEN 'OUTBOUND'
    WHEN tvendas.equipe LIKE '%OUVIDORIA%' THEN 'OUVIDORIA'
    WHEN tvendas.equipe LIKE '%RETENCAO%' THEN 'RETENCAO'
    WHEN tvendas.equipe LIKE '%CORPORATIVO%' THEN 'CORPORATIVO'
    WHEN tvendas.equipe LIKE '%COBRANCA%' THEN 'COBRANCA'
    WHEN tvendas.equipe LIKE '%MIGRACAO%' THEN 'MIGRACAO'
    WHEN tvendas.equipe LIKE '%RH%' THEN 'RH'
    WHEN tvendas.equipe LIKE '%TLV_ATIVO%' THEN 'ATIVO'
    WHEN tvendas.equipe LIKE '%RECEP%'
      OR tvendas.equipe LIKE '%CALL_VENDAS%'
      OR tvendas.equipe LIKE '%TELEVENDAS%' THEN 'TLV RECEPTIVO'
    WHEN tvendas.equipe LIKE '%PRODUTOS_MKT%'
      OR tvendas.equipe = 'MARKETING'  THEN 'MARKETING'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_CLICK%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_UNIVOX%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_INDEPENDENTE%' THEN 'GIGA EMBAIXADOR'
    WHEN tvendas.equipe LIKE '%EQUIPE_GIGA_EMBAIXADOR%' THEN 'GIGA EMBAIXADOR'
    ELSE 'SEM ALOCAÇÃO'
  END AS canal,
  tendereco.unidade,
  tleads.tipo_residencia,

  velocidade_contrato.velocidade,

  CASE
    WHEN UPPER(tvendas.pacote_base_nome) LIKE '%LIVRE%'
      OR UPPER(tvendas.pacote_base_nome) LIKE '%BRONZE%'
      OR UPPER(tvendas.pacote_base_nome) LIKE '%PRATA%'
      OR UPPER(tvendas.pacote_base_nome) LIKE '%OURO%' THEN 'COMBO'
    ELSE 'BANDA LARGA'
  END AS produto,
  CASE
    WHEN UPPER(tvendas.pacote_base_nome) LIKE '%LIVRE%' THEN 'LIVRE'
    WHEN UPPER(tvendas.pacote_base_nome) LIKE '%BRONZE%' THEN 'BRONZE'
    WHEN UPPER(tvendas.pacote_base_nome) LIKE '%PRATA%' THEN 'PRATA'
    WHEN UPPER(tvendas.pacote_base_nome) LIKE '%OURO%' THEN 'OURO'
    ELSE 'NAO_CONSTA'
  END AS pacote_tv,
  CASE
    WHEN tvendas.cod_tipo_cobranca LIKE '%DA%' THEN 'DEBITO_AUTOMATICO'
    WHEN tvendas.cod_tipo_cobranca LIKE '%BOL%' THEN 'BOLETO'
    ELSE 'OUTROS'
  END AS cobranca,
  CASE
    WHEN tvendas.equipe IN (
        'EQUIPE_COM_DIGITAL_VIRTUAL',
        'EQUIPE_PRODUTOS_MKT',
        'EQUIPE_RETENCAO',
        'EQUIPE_RH',
        'EQUIPE_TELEVENDAS',
        'EQUIPE_TLV_RECEPTIVO',
        'EQUIPE_PAP',
        'EQUIPE_PAP_BRASILIA',
        'EQUIPE_OUVIDORIA',
        'EQUIPE_MIGRACAO',
        'EQUIPE_CALL_VENDAS',
        'EQUIPE_LOJA',
        'EQUIPE_TLV_ATIVO',
        'EQUIPE_DIGITAL_BRASILIA',
        'EQUIPE_DIGITAL',
        'EQUIPE_CORPORATIVO',
        'EQUIPE_COBRANCA',
        'EQUIPE_DIGITAL_ASSINE',
        'LOJA'
      ) THEN 'PROPRIO'
    ELSE 'TERCEIRIZADO'
  END AS tipo_canal,
  valor_referencia_b2b,
  CASE
    WHEN COALESCE(
        tvendas.recorrencia_percentual_desconto,
        tdesconto.desconto
      ) > 0
      AND COALESCE(
        tvendas.recorrencia_percentual_desconto,
        tdesconto.desconto
      ) IS NOT NULL
      THEN COALESCE(
          tvendas.recorrencia_meses_desconto,
          TIMESTAMPDIFF(
            -- PONTO DE ATENÇÃO:
            MONTH,
            tdesconto.dia_aplicar,
            tdesconto.data_validade
          )
        )
    ELSE 0
  END AS duracao_desconto,
  CAST(COALESCE(cf.cpf, cj.cnpj) AS STRING) AS cpf_cnpj

FROM {{ ref('vw_air_tbl_venda') }} AS tvendas

LEFT JOIN {{ ref('vw_air_tbl_contrato') }} AS tcontrato ON (tvendas.id_contrato = tcontrato.id)

LEFT JOIN {{ ref('vw_air_tbl_cliente') }} AS tcliente ON (tcontrato.id_cliente = tcliente.id)

LEFT JOIN {{ ref('vw_air_tbl_cliente_fisico') }} AS cf ON (tcliente.id = cf.id_cliente)

LEFT JOIN {{ ref('vw_air_tbl_cliente_juridico') }} AS cj ON (tcliente.id = cj.id_cliente)

LEFT JOIN {{ ref('vw_air_tbl_endereco') }} AS tendereco ON (tvendas.id_endereco = tendereco.id)

LEFT JOIN {{ ref('vw_air_tbl_endereco_cidade') }} AS tendereco_cidade ON (tendereco.id_cidade = tendereco_cidade.id)

LEFT JOIN {{ ref('vw_air_tbl_unidade') }} AS tunidade

  ON (
    tendereco.unidade = tunidade.sigla
    AND tunidade.excluido = FALSE
  )
LEFT JOIN (
  SELECT a.*

  FROM {{ ref('vw_air_tbl_desconto') }} AS a

  INNER JOIN (
    SELECT
      id_contrato,
      MIN(id) AS id

    FROM {{ ref('vw_air_tbl_desconto') }}

    WHERE categoria = 'CAMPANHA'
    GROUP BY 1
  ) AS b ON a.id_contrato = b.id_contrato
  AND a.id = b.id
) AS tdesconto
  ON (
    tvendas.id_contrato = tdesconto.id_contrato
    AND tvendas.pacote_base_codigo = tdesconto.item_codigo
    AND tdesconto.categoria = 'CAMPANHA'
  )

LEFT JOIN {{ ref('vw_air_tbl_vendedor') }} AS tvendedor ON tvendas.id_vendedor = tvendedor.id

LEFT JOIN {{ ref('vw_air_tbl_usuario') }} AS tusuario ON tvendedor.usuario = tusuario.codigo

LEFT JOIN {{ ref('vw_air_tbl_campanha') }} AS tcampanha ON tvendas.id_campanha = tcampanha.id

LEFT JOIN {{ ref('vw_air_tbl_lead') }} AS tleads ON tvendas.id_lead = tleads.id

LEFT JOIN (
  SELECT
    id_contrato,
    MAX(id) AS id

  FROM {{ ref('vw_air_tbl_contrato_entrega') }}

  GROUP BY id_contrato
) AS ce ON tcontrato.id = ce.id_contrato

LEFT JOIN {{ ref('vw_air_tbl_contrato_entrega') }} AS ce2 ON ce.id = ce2.id

LEFT JOIN {{ ref('vw_air_tbl_endereco') }} AS tendereco_entrega ON ce2.id_endereco = tendereco_entrega.id

LEFT JOIN velocidade_contrato ON tcontrato.id = velocidade_contrato.id_contrato AND tcontrato.versao = velocidade_contrato.versao_contrato

WHERE tvendas.natureza = 'VN_NOVO_CONTRATO'
  AND ((UPPER(tvendas.equipe) <> 'EQUIPE_MIGRACAO') OR (UPPER(tvendas.equipe) = 'EQUIPE_MIGRACAO' AND UPPER(tcontrato.legado_sistema) = 'URBE'))
  AND tvendas.id_contrato IS NOT NULL
  AND (tcontrato.data_criacao >= '2023-01-01' OR tcontrato.status <> 'ST_CONT_CANCELADO')
  AND UPPER(TRIM(tcliente.nome)) NOT LIKE '%SUMICITY%'
  AND UPPER(TRIM(tcliente.nome)) NOT LIKE '%VM OPENLINK%'
  AND UPPER(TRIM(tcliente.nome)) NOT LIKE '%VELOMAX%'
