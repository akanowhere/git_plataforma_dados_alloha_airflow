WITH filas AS (
    SELECT id_fila,
            nome AS fila,
            CASE
                WHEN nome like '%ANÁLISE - PACOTES E SERVIÇOS%' THEN 1
                WHEN nome like '%ANÁLISE - GIGA+ TV%' THEN 1
                ELSE 0
            END fila_produto
    FROM gold.chamados.dim_fila
)
,vendas AS (
  SELECT v.id_contrato,
         v.data_criacao,
         COALESCE(LEAD(v.data_criacao) OVER (PARTITION BY v.id_contrato ORDER BY v.data_criacao),CURRENT_TIMESTAMP()) AS proxima_data,
         v.pacote_base_nome,
         CASE 
            WHEN natureza = 'VN_NOVO_CONTRATO' THEN 1
            ELSE m.novo_versao 
         END AS versao  
  FROM gold.venda.dim_venda_geral v
  LEFT JOIN gold.base.dim_migracao m ON v.id_contrato = m.id_contrato AND v.id_venda = m.id_venda
  WHERE v.natureza in ('VN_MIGRACAO_UP','VN_NOVO_CONTRATO','VN_MIGRACAO_DOWN','VN_MIGRACAO_COMPULSORIA') and v.cancelada = false and v.excluido = false
)
, atendimentos AS (
  SELECT ct.id_chamado AS protocolo,
      ct.id_fila_origem AS id_fila_atendimento, 
      ct.codigo_usuario AS usuario_atendimento,
      COALESCE(LAG(ct.data_transferencia) OVER (PARTITION BY ct.id_chamado ORDER BY ct.id ASC),dc.data_abertura) AS data_inicio_atendimento,
      ct.data_transferencia AS data_fim_atendimento
  FROM gold.chamados.dim_chd_transferencia ct
  LEFT JOIN gold.chamados.dim_chamado dc ON ct.id_chamado = dc.id
  UNION ALL
  SELECT id AS protocolo,
        id_fila AS id_fila_atendimento,
        usuario_atribuido AS usuario_atendimento,
        COALESCE(data_transferencia,data_abertura) AS data_inicio_atendimento,
        data_conclusao AS data_fim_atendimento   
  FROM gold.chamados.dim_chamado dc
)
,atendimentos_completos AS (
    SELECT  dc.id as protocolo,
            dc.codigo_contrato AS contrato,
            dc.endereco_unidade AS unidade,
            e.cidade,
            e.estado,
            dc.nome_classificacao,
            dc.tipo_classificacao,
            CASE
                WHEN dc.data_conclusao IS NOT NULL THEN 'Concluído'
                ELSE 'Aberto'
            END status_chamado,
            dc.motivo_conclusao,
            dc.data_abertura AS data_abertura_chamado,
            dc.data_conclusao AS data_conclusao_chamado,
            u1.email AS usuario_abertura_chamado, 
            u2.email AS usuario_conclusao_chamado,   
            ROW_NUMBER() OVER (PARTITION BY dc.id ORDER BY a.data_inicio_atendimento ASC) AS numero_atendimento,
            a.id_fila_atendimento,
            dc.nome_fila AS fila_atual,        
            a.data_inicio_atendimento AS data_inicio_atendimento,
            a.data_fim_atendimento AS data_fim_atendimento,
            a.usuario_atendimento AS codigo_usuario_atendimento,
            u3.email AS usuario_atendimento,
            v.pacote_base_nome as nome_pacote

    FROM gold.chamados.dim_chamado dc
    LEFT JOIN atendimentos a ON dc.id = a.protocolo
    LEFT JOIN filas f ON a.id_fila_atendimento = f.id_fila
    LEFT JOIN gold.base.dim_contrato c ON dc.codigo_contrato = c.id_contrato
    LEFT JOIN gold.base.dim_endereco e ON c.id_endereco_cobranca = e.id_endereco
    LEFT JOIN gold.base.dim_usuario u1 ON dc.usuario_abertura = u1.codigo
    LEFT JOIN gold.base.dim_usuario u2 ON dc.usuario_atribuido = u2.codigo
    LEFT JOIN gold.base.dim_usuario u3 ON a.usuario_atendimento = u3.codigo
    LEFT JOIN vendas v ON dc.codigo_contrato = v.id_contrato AND to_timestamp(dc.data_abertura,'yyyy-MM-dd HH:mm:ss') BETWEEN v.data_criacao AND v.proxima_data
    WHERE f.fila_produto = 1 AND dc.data_abertura >= '2024-01-01' 
) 
,cte_texto AS (
    SELECT 
        t2.id_chamado,
        t2.id_fila,
        t2.usuario_criacao,
        t2.texto,
        ROW_NUMBER() OVER (
            PARTITION BY t2.id_chamado, t2.id_fila, t2.usuario_criacao 
            ORDER BY t2.data_criacao DESC
        ) AS rn,
         ROW_NUMBER() OVER (
            PARTITION BY t2.id_chamado
            ORDER BY t2.data_criacao ASC
        ) AS rn_chamado
    FROM gold.chamados.dim_chd_descricao t2
    WHERE t2.excluido = false 
)
,cte_base_chamados_completa AS (
SELECT t1.*,
       (SELECT max(data_abertura_chamado) FROM atendimentos_completos WHERE t1.contrato = contrato AND protocolo < t1.protocolo) data_ultimo_chamado,
       t2.texto as ultima_observacao_atendimento,
       t3.texto as primeira_observacao_chamado
FROM atendimentos_completos t1
LEFT JOIN cte_texto t2 ON t1.protocolo = t2.id_chamado AND t1.id_fila_atendimento = t2.id_fila AND t1.codigo_usuario_atendimento = t2.usuario_criacao AND t2.rn = 1
LEFT JOIN cte_texto t3 ON t1.protocolo = t3.id_chamado AND t3.rn_chamado = 1
ORDER BY t1.contrato,t1.protocolo DESC, t1.numero_atendimento DESC
)
SELECT * ,
      CASE
          WHEN (unix_timestamp(data_abertura_chamado) - unix_timestamp(data_ultimo_chamado)) < 86.400 THEN 1
          ELSE 0
     END flag_recorrencia_24h,
     CASE
          WHEN DATE_DIFF(data_abertura_chamado,data_ultimo_chamado) < 604.800 THEN 1
          ELSE 0
     END flag_recorrencia_7d,
     CURRENT_DATE()-1 AS data_referencia
FROM cte_base_chamados_completa
ORDER BY DATE_DIFF(data_abertura_chamado,data_ultimo_chamado) DESC
