WITH calendario AS (
    SELECT t.DataDate AS data,
           CASE
                WHEN f.data IS NOT NULL THEN 0
                WHEN t.DiaNome = 'Domingo' THEN 0
                ELSE 1
           END util      
    FROM gold.auxiliar.dim_tempo t
    LEFT JOIN gold.auxiliar.feriados f ON t.DataDate = f.data
    WHERE t.DataDate >= '2023-01-01'
)
,filas AS (
    SELECT id_fila,
            nome AS fila,
            CASE
                WHEN id_fila in (36,55,59,64,67,78,151) THEN 'OUTRAS'
                WHEN id_fila in (175,204) THEN 'FINANCEIRO'
                WHEN UPPER(nome) LIKE '%FINANCEIRO%' THEN 'FINANCEIRO'
                ELSE 'OUTRAS'
            END classificacao
    FROM gold.chamados.dim_fila
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
  FROM gold.chamados.dim_chamado
)
,atendimentos_completos AS (
    SELECT  dc.id as protocolo,
            dc.codigo_contrato AS contrato,
            dc.endereco_unidade AS unidade,
            du.marca,
            du.regional,
            du.nome AS cidade,
            ROW_NUMBER() OVER (PARTITION BY dc.id ORDER BY a.data_inicio_atendimento ASC) AS n_atendimento,        
            CAST(a.data_inicio_atendimento AS DATE) AS data_inicio_atendimento,
            a.data_inicio_atendimento AS datahora_inicio_atendimento,
            CAST(a.data_fim_atendimento AS DATE) AS data_fim_atendimento,
            COALESCE(a.data_fim_atendimento,DATE_FORMAT(CURRENT_DATE()-1,'yyyy-MM-dd 23:59:59')) AS data_final,
            a.data_fim_atendimento AS datahora_fim_atendimento,
            a.usuario_atendimento,
            u3.email AS email_usuario_atendimento_chamado,  
            f.fila AS fila_atendimento,
            f.classificacao,
            dc.usuario_abertura AS usuario_abertura_chamado,
            u1.email AS email_usuario_abertura_chamado,        
            dc.usuario_atribuido AS usuario_conclusao_chamado,
            u2.email AS email_usuario_conclusao_chamado,            
            CAST(dc.data_abertura AS DATE) AS data_abertura_chamado,
            dc.data_abertura AS datahora_abertura_chamado,
            CAST(dc.data_conclusao AS DATE) AS data_conclusao_chamado,
            dc.data_conclusao AS datahora_conclusao_chamado,
            DATE_DIFF(COALESCE(dc.data_conclusao,CURRENT_DATE()-1),dc.data_abertura) AS sla_chamado,
            CASE
                WHEN  a.data_fim_atendimento = dc.data_conclusao THEN 'Concluido'
                WHEN  a.data_fim_atendimento IS NOT NULL THEN 'Transferida'
                ELSE 'Aguardando Atendimento'
            END status_chamado,
            dc.nome_classificacao AS motivo_abertura,
            dc.tipo_classificacao,
            dc.motivo_conclusao,
            '' AS observacoes
    FROM gold.chamados.dim_chamado dc
    LEFT JOIN atendimentos a ON dc.id = a.protocolo
    LEFT JOIN filas f ON a.id_fila_atendimento = f.id_fila
    LEFT JOIN gold.base.dim_unidade du ON dc.endereco_unidade = du.sigla AND du.excluido = false
    LEFT JOIN gold.base.dim_usuario u1 ON dc.usuario_abertura = u1.codigo
    LEFT JOIN gold.base.dim_usuario u2 ON dc.usuario_atribuido = u2.codigo
    LEFT JOIN gold.base.dim_usuario u3 ON a.usuario_atendimento = u3.codigo
    WHERE f.classificacao = 'FINANCEIRO' AND dc.data_abertura >= TRUNC(ADD_MONTHS(CURRENT_DATE(), -12), 'MM')
)
,sla_atendimento AS (
    SELECT  a.protocolo,
            a.n_atendimento,
            a.datahora_inicio_atendimento,
            a.datahora_fim_atendimento,
            SUM(CASE
                    WHEN CAST(a.datahora_inicio_atendimento AS DATE) = CAST(a.data_final AS DATE) THEN (unix_timestamp(a.data_final) - unix_timestamp(a.datahora_inicio_atendimento))*c.util
                    WHEN CAST(a.data_final AS DATE) = c.data THEN (unix_timestamp(a.data_final) - unix_timestamp(DATE_FORMAT(a.data_final,'yyyy-MM-dd 00:00:00')))*c.util
                    WHEN CAST(a.datahora_inicio_atendimento AS DATE) = c.data THEN (unix_timestamp(DATE_FORMAT(a.datahora_inicio_atendimento,'yyyy-MM-dd 23:59:59')) - unix_timestamp(a.datahora_inicio_atendimento))*c.util
                    ELSE 86400*c.util
            END) AS duracao_util_atendimento
    FROM atendimentos_completos a
    LEFT JOIN calendario c ON (c.data between a.data_inicio_atendimento AND a.data_final)
    GROUP BY  a.protocolo,
              a.n_atendimento,
              a.datahora_inicio_atendimento,
              a.datahora_fim_atendimento
)
SELECT a.*,
       s.duracao_util_atendimento, 
       CASE 
        WHEN s.duracao_util_atendimento > 172800 THEN 'Mais de 48h' 
        ELSE 'Até 48h' 
      END categoria_sla_atendimento_48h,
      CASE 
        WHEN duracao_util_atendimento <= 86400 THEN 'Até 1 dia'
        WHEN duracao_util_atendimento > 86400 AND duracao_util_atendimento <= 172800 THEN '1-2 dias'
        WHEN duracao_util_atendimento > 172800 AND duracao_util_atendimento <= 432000 THEN '2-5 dias'
        WHEN duracao_util_atendimento > 432000 AND duracao_util_atendimento <= 864000 THEN '5-10 dias'
        WHEN duracao_util_atendimento > 864000 AND duracao_util_atendimento <= 1296000 THEN '10-15 dias'
        ELSE 'Mais de 15 dias'
    END AS categoria_sla_atendimento
FROM atendimentos_completos a
LEFT JOIN sla_atendimento s ON a.protocolo = s.protocolo AND a.n_atendimento = s.n_atendimento