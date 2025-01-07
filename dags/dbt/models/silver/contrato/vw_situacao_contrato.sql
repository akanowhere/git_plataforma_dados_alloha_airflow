SELECT DISTINCT *
FROM
  (
    SELECT
      c.id AS id_contrato,
      status_contrato.nome AS status,
      DATE(c.data_primeira_ativacao) AS data_ativacao,
      DATE(c.data_cancelamento) AS data_cancelamento,
      IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) AS motivo_cancelamento,
      CASE
        WHEN c.data_cancelamento IS NULL THEN NULL
        WHEN c.data_primeira_ativacao IS NULL THEN 'CANCELADO_ANTES_ATIVACAO'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%Cancelado por Débito%' THEN 'INVOLUNTARIO'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%FPD%' THEN 'INVOLUNTARIO'
        ELSE 'VOLUNTARIO'
      END AS tipo_cancelamento,
      CASE
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%Cancelado por Débito%'
          AND retencao.usuario_alteracao IS NULL THEN 'SISTEMICO'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%Cancelado por Débito%'
          AND retencao.usuario_alteracao IS NOT NULL THEN 'MANUAL'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%FPD%'
          AND retencao.usuario_alteracao IS NULL THEN 'SISTEMICO'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%FPD%'
          AND retencao.usuario_alteracao IS NOT NULL THEN 'MANUAL'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%ERRO_MIGRACAO%'
          AND retencao.usuario_alteracao IS NULL THEN 'SISTEMICO'
        WHEN IFNULL(tipo_encerramento.nome, c.cancelamento_motivo) LIKE '%ERRO_MIGRACAO%'
          AND retencao.usuario_alteracao IS NOT NULL THEN 'MANUAL'
        ELSE NULL
      END AS cancelamento_invol,
      DATE(c.data_criacao) AS data_cadastro_sistema,
      IFNULL(
      REPLACE(REPLACE(REPLACE(UPPER({{ translate_column('tendereco_entrega.cidade') }}), CHAR(13), ''), CHAR(10), ''), "'", " "),
      REPLACE(REPLACE(REPLACE(UPPER({{ translate_column('tendereco_cobranca.cidade') }}), CHAR(13), ''), CHAR(10), ''), "'", " ")
      ) AS cidade,
      NULL AS regiao,
      IFNULL(
        tendereco_entrega.estado,
        tendereco_cobranca.estado
      ) AS estado,
      CASE
        WHEN tvendas.equipe IN ('EQUIPE_PAP', 'EQUIPE_PAP_BRASILIA') THEN 'PAP DIRETO'
        WHEN tvendas.equipe = 'EQUIPE_TLV_ATIVO' THEN 'ATIVO'
        WHEN tvendas.equipe IN ('EQUIPE_AVANT_TELECOM_EIRELI', 'EQUIPE_BLISS_TELECOM', 'EQUIPE_FIBRA_TELECOM',
                            'EQUIPE_RR_TELECOM', 'EQUIPE_MAKOTO_TELECOM', 'EQUIPE_MORANDI',
                            'EQUIPE_LITORAL_SAT')
        THEN 'PAP INDIRETO'
        WHEN tvendas.equipe LIKE '%PAP%' THEN 'PAP INDIRETO'
        WHEN tvendas.equipe LIKE '%AGENTE_DIGITAL%' THEN 'AGENTE DIGITAL'
        WHEN tvendas.equipe LIKE '%DIGITAL_ASSINE%' THEN 'ASSINE'
        WHEN tvendas.equipe LIKE 'EQUIPE_DIGITAL' OR tvendas.equipe LIKE 'EQUIPE_DIGITAL_BRA%' OR
          tvendas.equipe LIKE '%COM_DIGITAL%' THEN 'DIGITAL'
        WHEN tvendas.equipe LIKE '%LOJA%' THEN 'LOJA'
        WHEN tvendas.equipe LIKE '%OUTBOUND%' THEN 'OUTBOUND'
        WHEN tvendas.equipe LIKE '%OUVIDORIA%' THEN 'OUVIDORIA'
        WHEN tvendas.equipe LIKE '%RETENCAO%' THEN 'RETENCAO'
        WHEN tvendas.equipe LIKE '%CORPORATIVO%' THEN 'CORPORATIVO'
        WHEN tvendas.equipe LIKE '%COBRANCA%' THEN 'COBRANCA'
        WHEN tvendas.equipe LIKE '%MIGRACAO%' THEN 'MIGRACAO'
        WHEN tvendas.equipe LIKE '%RH%' THEN 'RH'
        WHEN tvendas.equipe LIKE '%TLV_ATIVO%' THEN 'ATIVO'
        WHEN tvendas.equipe LIKE '%RECEP%' OR tvendas.equipe LIKE '%CALL_VENDAS%' OR
          tvendas.equipe LIKE '%TELEVENDAS%' THEN 'RECEPTIVO'
        WHEN tvendas.equipe LIKE '%PRODUTOS_MKT%' THEN 'MARKETING'
        WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_CLICK%' THEN 'GIGA EMBAIXADOR'
        WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA%' THEN 'GIGA EMBAIXADOR'
        WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_UNIVOX%' THEN 'GIGA EMBAIXADOR'
        WHEN tvendas.equipe LIKE '%EQUIPE_CONSULTOR_INDEPENDENTE%' THEN 'GIGA EMBAIXADOR'
        WHEN tvendas.equipe LIKE 'EQUIPE_GIGA_EMBAIXADOR' THEN 'GIGA EMBAIXADOR'
        WHEN cpm.contrato_air IS NOT NULL THEN 'INDIQUE UM AMIGO'
        ELSE 'SEM ALOCAÇÃO'
      END AS canal,
      COALESCE(
        REPLACE(tvendas.equipe, 'EQUEQUIPE_', 'EQUIPE_'),
        'SEM ALOCAÇÃO'
      ) AS equipe,
      IFNULL(
        IFNULL(
          tendereco_entrega.marca,
          tendereco_cobranca.marca
        ),
        tu.marca
      ) AS marca,
      IFNULL(
        tendereco_entrega.unidade,
        tendereco_cobranca.unidade
      ) AS unidade_atendimento,
      IFNULL(
        tendereco_entrega.regional,
        tendereco_cobranca.regional
      ) AS regional,
      CASE
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%LIVRE%'
          OR UPPER(tvendas.pacote_base_nome) LIKE '%BRONZE%'
          OR UPPER(tvendas.pacote_base_nome) LIKE '%PRATA%'
          OR UPPER(tvendas.pacote_base_nome) LIKE '%OURO%' THEN 'COMBO'
        ELSE 'BANDA LARGA'
      END AS produto,
      CASE
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%LIVRE%' THEN 'CONSTA'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%BRONZE%' THEN 'CONSTA'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%PRATA%' THEN 'CONSTA'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%OURO%' THEN 'CONSTA'
        ELSE 'NAO CONSTA'
      END AS pacote_tv,
      CASE
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%LIVRE%' THEN 'LIVRE'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%BRONZE%' THEN 'BRONZE'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%PRATA%' THEN 'PRATA'
        WHEN UPPER(tvendas.pacote_base_nome) LIKE '%OURO%' THEN 'OURO'
        ELSE NULL
      END AS tipo_tv,
      --'SUMICITY' AS marca
      CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
      tcf.fechamento AS dia_fechamento,
      tcf.dia AS dia_vencimento,
      CASE
        WHEN c.pacote_nome LIKE '%URBE%' THEN 'B2C'
        WHEN c.b2b = TRUE THEN 'B2B'
        WHEN c.pme = TRUE THEN 'PME'
        ELSE 'B2C'
      END AS segmento,
      COALESCE(
        rrr.sub_motivo_cancelamento,
        retencao.sub_motivo_cancelamento
      ) AS sub_motivo_cancelamento,
      c.data_alteracao AS ultima_alteracao,
      c.valor_final AS valor_contrato,
      c.pacote_nome AS plano,
      'data_primeira_ativacao' AS data_utilizada,
      NULL AS motivo_inclusao,
      tvendas.id_vendedor,
      tu.cluster,
      c.legado_id,
      c.legado_sistema,
      'AIR' AS fonte,
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
      CASE
        WHEN c.data_criacao IS NOT NULL
          AND c.data_cancelamento IS NOT NULL
          AND c.legado_sistema IS NOT NULL
          AND c.data_cancelamento < c.data_criacao THEN 'Sem Informacao'
        WHEN c.data_primeira_ativacao IS NOT NULL
          AND desconto.id_contrato IS NOT NULL THEN 'sim'
        WHEN c.data_primeira_ativacao IS NOT NULL
          AND mig.natureza = 'VN_MIGRACAO_COMPULSORIA' THEN 'nao'
        WHEN c.data_primeira_ativacao IS NOT NULL
          AND c.`status` <> 'ST_CONT_CANCELADO'
          AND (
            mig.natureza <> 'VN_MIGRACAO_COMPULSORIA'
            OR mig.natureza IS NULL
          )
          AND DATEDIFF(
            CAST(CURRENT_DATE() AS DATE),
            IFNULL(
              mig.data_venda,
              c.data_primeira_ativacao
            )
          ) BETWEEN 0
          AND 365 THEN 'sim'
        WHEN c.data_primeira_ativacao IS NOT NULL
          AND c.`status` = 'ST_CONT_CANCELADO'
          AND (
            mig.natureza <> 'VN_MIGRACAO_COMPULSORIA'
            OR mig.natureza IS NULL
          )
          AND DATEDIFF(
            c.data_cancelamento,
            IFNULL(
              mig.data_venda,
              c.data_primeira_ativacao
            )
          ) BETWEEN 0
          AND 365 THEN 'sim'
        ELSE 'nao'
      END AS flg_fidelizado
    FROM
      {{ get_catalogo('bronze') }}.air_comercial.tbl_contrato AS c
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_vencimento AS tcf ON c.id_vencimento = tcf.id
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_catalogo_item AS status_contrato ON c.status = status_contrato.codigo
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_catalogo_item AS tipo_encerramento ON c.cancelamento_motivo = tipo_encerramento.codigo
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_retencao AS rrr
      ON (
        c.id = rrr.id_contrato
        AND rrr.status = 'RT_CANCELADO'
      )
    LEFT JOIN (
      SELECT
        ce.id_contrato,
        e.unidade,
        cd.nome AS cidade,
        cd.estado,
        u.marca,
        REPLACE(
          REPLACE(u.regional, 'EGIAO_0', ''),
          'EGIAO-0',
          ''
        ) AS regional
      FROM
        {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_entrega AS ce
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco AS e ON (ce.id_endereco = e.id)
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco_cidade AS cd ON (e.id_cidade = cd.id)
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade AS u
        ON (
          e.unidade = u.sigla
          AND u.excluido = FALSE
        )
      WHERE
        ce.excluido = FALSE
        AND ce.data_criacao = (
          SELECT MAX(data_criacao)
          FROM
            {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_entrega
          WHERE
            ce.excluido = FALSE
            AND id_contrato = ce.id_contrato
        )
    ) AS tendereco_entrega ON c.id = tendereco_entrega.id_contrato
    LEFT JOIN (
      SELECT
        ce.id AS id_contrato,
        e.unidade,
        cd.nome AS cidade,
        cd.estado,
        u.marca,
        REPLACE(
          REPLACE(u.regional, 'EGIAO_0', ''),
          'EGIAO-0',
          ''
        ) AS regional
      FROM
        {{ get_catalogo('bronze') }}.air_comercial.tbl_contrato AS ce
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco AS e
        ON (
          ce.id_endereco_cobranca = e.id
        )
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco_cidade AS cd ON (e.id_cidade = cd.id)
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade AS u
        ON (
          e.unidade = u.sigla
          AND u.excluido = FALSE
        )
      WHERE
        ce.excluido = FALSE
        AND ce.data_criacao = (
          SELECT MAX(data_criacao)
          FROM
            {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_entrega
          WHERE
            ce.excluido = FALSE
            AND id_contrato = ce.id
        )
    ) AS tendereco_cobranca ON c.id = tendereco_cobranca.id_contrato
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_venda AS tvendas
      ON c.id = tvendas.id_contrato
      AND tvendas.natureza = 'VN_NOVO_CONTRATO'
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_vendedor AS tvendedor ON tvendas.id_vendedor = tvendedor.id
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_cliente AS tcliente ON c.id_cliente = tcliente.id
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_retencao AS retencao
      ON (
        c.id = retencao.id_contrato
        AND retencao.status = 'RT_CANCELADO'
      )
    LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_unidade AS tu
      ON tu.sigla = IFNULL(
        tendereco_entrega.unidade,
        IFNULL(
          tendereco_cobranca.unidade,
          c.unidade_atendimento
        )
      )
      AND tu.excluido = FALSE
    LEFT JOIN (
      SELECT
        cm.id_contrato,
        cm.novo_versao,
        v.natureza,
        v.equipe,
        v.data_venda
      FROM
        {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_migracao AS cm
      LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_venda AS v ON (cm.id_venda = v.id)
      WHERE
        cm.reprovada = FALSE
        AND cm.id = (
          SELECT MAX(id)
          FROM
            {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_migracao
          WHERE
            reprovada = FALSE
            AND id_contrato = cm.id_contrato
        )
    ) AS mig
      ON (
        c.id = mig.id_contrato
        AND c.versao = mig.novo_versao
      )
    LEFT JOIN (
      SELECT
        d.id,
        d.id_contrato,
        d.desconto,
        d.dia_aplicar,
        d.data_validade
      FROM
        {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto AS d
      WHERE
        d.categoria = 'RETENCAO'
        AND CAST(d.dia_aplicar AS DATE) >= '2023-07-01'
        AND d.id IN (
          SELECT MAX(id)
          FROM
            {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto
          WHERE
            categoria = 'RETENCAO'
            AND CAST(dia_aplicar AS DATE) >= '2023-07-01'
            AND id_contrato = d.id_contrato
        )
        AND d.id NOT IN (
          SELECT id
          FROM
            {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto
          WHERE
            categoria = 'RETENCAO'
            AND CAST(dia_aplicar AS DATE) >= '2023-07-01'
            AND (
              desconto BETWEEN 09
              AND 11
              AND DATEDIFF(data_validade, dia_aplicar) < 55
            )
        )
    ) AS desconto ON c.id = desconto.id_contrato
    LEFT JOIN (
      SELECT DISTINCT contrato_air
      FROM {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_cupom
    ) AS cpm ON cpm.contrato_air = c.id
    WHERE
      TRIM(tcliente.nome) NOT LIKE '%SUMICITY%'
      AND TRIM(tcliente.nome) NOT LIKE '%VM OPENLINK%'
      AND TRIM(tcliente.nome) NOT LIKE '%VELOMAX%'
  ) AS b
