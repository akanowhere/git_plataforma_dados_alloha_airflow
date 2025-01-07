WITH evento_alt_vencimento AS (
    SELECT id_evento,
    momento,
    id_contrato AS ID_CONTRATO_AIR,
    motivo_associado,
    tipo,
    Observacao
    FROM {{ ref('dim_cliente_evento') }} as dim_cliente_evento
    WHERE UPPER(tipo) = 'EVT_CONTRATO_VENCIMENTO_ALTERADO'
)

select evento_alt_vencimento.id_evento,
evento_alt_vencimento.momento,
evento_alt_vencimento.ID_CONTRATO_AIR,
evento_alt_vencimento.motivo_associado,
evento_alt_vencimento.tipo,
REPLACE(
        SUBSTRING(
            Observacao,
            CHARINDEX('Vencimento anterior: [Fechamento (', Observacao) + 34,
            2
        ),
        ')',
        ''
    ) AS Fechamento_Antigo,
    REPLACE(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    Observacao,
                    CHARINDEX('Vencimento anterior: [Fechamento (', Observacao) + 50,
                    3
                ),
                ')',
                ''
            ),
            '(',
            ''
        ),
        ']',
        ''
    ) AS Vencimento_Antigo,
    REPLACE(
        SUBSTRING(
            Observacao,
            CHARINDEX('Vencimento atual: [Fechamento (', Observacao) + 31,
            2
        ),
        ')',
        ''
    ) AS Fechamento_Novo,
    REPLACE(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    Observacao,
                    CHARINDEX('Vencimento atual: [Fechamento (', Observacao) + 47,
                    3
                ),
                ')',
                ''
            ),
            '(',
            ''
        ),
        ']',
        ''
    ) AS Vencimento_Novo
FROM evento_alt_vencimento