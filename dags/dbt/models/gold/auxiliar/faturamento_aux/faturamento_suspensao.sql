WITH eventos_suspensao AS (
    SELECT id_evento,
    momento,
    id_contrato AS ID_CONTRATO_AIR,
    motivo_associado,
    tipo,
    observacao
    FROM {{ ref('dim_cliente_evento') }}
    WHERE
    (UPPER(tipo) LIKE '%HABILIT%'
    OR UPPER(tipo) LIKE '%ATIV%'
    OR UPPER(tipo) LIKE '%SUSP%'
    OR UPPER(tipo) IN ('EVT_CONTRATO_HABILITADO_ENVIO_COMPROVANTE',
                'EVT_CONTRATO_HABILITADO_APOS_PAGAMENTO',
                'EVT_CONTRATO_HABILITADO_SOLICITACAO',
                'EVT_CONTRATO_HABILITADO_CONFIANCA',
                'EVT_CONTRATO_SERVICOS_ATIVADOS',
                'EVT_CONTRATO_SUSPENSO',
                'EVT_CONTRATO_RETIDO',
                'EVT_CONTRATO_HABILITADO_AUTOMATICAMENTE',
                'EVT_ATIVACAO_WOC',
                'EVT_CONTRATO_CANCELAMENTO_SOLICITADO'))
),

-----------------------------
--- Criar Indices de Eventos
-----------------------------
Cliente_Evento_Aux_novo AS(
    SELECT ROW_NUMBER() OVER (ORDER BY ID_CONTRATO_AIR ASC, momento ASC, ID_EVENTO ASC) AS ID_EVENTO_AUXILIAR,
        ID_CONTRATO_AIR,
        momento,
        ID_EVENTO,
        tipo
    FROM eventos_suspensao
),

suspensao AS (
    SELECT ROW_NUMBER() OVER (ORDER BY Cliente_Evento_Aux_novo.ID_EVENTO_AUXILIAR ASC) AS ID_CONTRATO_SUSPENSAO,
    Cliente_Evento_Aux_novo.ID_CONTRATO_AIR,
    Cliente_Evento_Aux_novo.momento AS dtSuspensao,
    TBL_TEMP.momento AS dtAtivacao,
    Cliente_Evento_Aux_novo.ID_EVENTO AS ID_EVENTO_SUSPENSAO,
    TBL_TEMP.ID_EVENTO AS ID_EVENTO_ATIVACAO
    FROM Cliente_Evento_Aux_novo
    LEFT JOIN  Cliente_Evento_Aux_novo TBL_TEMP ON
    Cliente_Evento_Aux_novo.ID_EVENTO_AUXILIAR = (TBL_TEMP.ID_EVENTO_AUXILIAR -1) AND
    Cliente_Evento_Aux_novo.ID_CONTRATO_AIR = TBL_TEMP.ID_CONTRATO_AIR AND
    (UPPER(TBL_TEMP.tipo) like '%HABILIT%' OR UPPER(TBL_TEMP.tipo) like '%ATIV%' OR
    UPPER(TBL_TEMP.tipo) IN ('EVT_CONTRATO_HABILITADO_ENVIO_COMPROVANTE',
                    'EVT_CONTRATO_HABILITADO_APOS_PAGAMENTO',
                    'EVT_CONTRATO_HABILITADO_SOLICITACAO',
                    'EVT_CONTRATO_HABILITADO_CONFIANCA',
                    'EVT_CONTRATO_SERVICOS_ATIVADOS',
                    'EVT_CONTRATO_RETIDO',
                    'EVT_CONTRATO_HABILITADO_AUTOMATICAMENTE',
                    'EVT_ATIVACAO_WOC'))
    WHERE ( UPPER(Cliente_Evento_Aux_novo.tipo) IN ( 'EVT_CONTRATO_SUSPENSO', 'EVT_CONTRATO_CANCELAMENTO_SOLICITADO')
    OR UPPER(Cliente_Evento_Aux_novo.tipo) LIKE '%SUSP%' )
),

suspensao_ajustada AS (
    SELECT
    Suspensao.ID_EVENTO_SUSPENSAO
    FROM suspensao
    INNER JOIN suspensao TB_TEMP ON
    TB_TEMP.ID_CONTRATO_AIR = Suspensao.ID_CONTRATO_AIR AND
    TB_TEMP.dtSuspensao >= Suspensao.dtSuspensao
    WHERE Suspensao.ID_EVENTO_ATIVACAO IS NULL
    GROUP by Suspensao.ID_EVENTO_SUSPENSAO
)

SELECT ID_CONTRATO_SUSPENSAO,
       ID_CONTRATO_AIR,
       CAST(dtSuspensao AS TIMESTAMP) AS dtSuspensao,
       CASE WHEN suspensao_ajustada.ID_EVENTO_SUSPENSAO IS NOT NULL
            THEN CAST(dtSuspensao AS TIMESTAMP)
            ELSE CAST(dtAtivacao AS TIMESTAMP)
       END AS dtAtivacao,
       suspensao.ID_EVENTO_SUSPENSAO,
       ID_EVENTO_ATIVACAO
FROM suspensao
LEFT JOIN suspensao_ajustada
       ON suspensao.ID_EVENTO_SUSPENSAO = suspensao_ajustada.ID_EVENTO_SUSPENSAO
