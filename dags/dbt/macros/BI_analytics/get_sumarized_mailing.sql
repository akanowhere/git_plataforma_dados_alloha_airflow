{% macro get_sumarized_mailing(date_reference) %}
    {# 
        Macro para obter um resumo das faturas com base na data de referência fornecida.

        Parâmetros:
        - date_reference: Data de referência para filtrar os registros no formato adequado para SQL.

        Retorno:
        - SQL Query: Uma consulta SQL que retorna um resumo das faturas com as seguintes colunas:
        - MAILING: Faixa de aging ajustada.
        - CONTRATO_AIR: ID do contrato.
        - TIPO: Tipo de mailing.
        - DATA_INI: Data de início, que é a data de referência.
        - DATA_FIM: Data de fim, que é um dia antes da data de saída.
        - MARCA: Marca associada ao contrato.
        - NEGOCIACAO: Detalhes da negociação.
        - ULTIMA_CHANCE: Indica se é a última chance (SIM/NÃO).
        - motivo_saida: Motivo de saída.
        - data_cancelamento_contrato: Data de cancelamento do contrato.
        - data_pagamento: Data de pagamento.
        - REGIONAL: Regional associada.
        - CLUSTER: Cluster associado.
        - FATURAS_ABERTAS: Número de faturas abertas.
        - FATURA_MOTIVADORA: Fatura motivadora.

        Exemplo:
        {% set query = get_sumarized_mailing('2024-08-15') %}
    #}

    SELECT  
        REPLACE(REPLACE(t1.faixa_aging,'[ULTIMA_CHANCE]',''),'61-90d+','61-90d') AS MAILING,
        t1.id_contrato AS CONTRATO_AIR,
        t1.status_mailing AS TIPO,
        t1.data_referencia AS DATA_INI,
        DATEADD(DAY,-1, t1.data_saida) AS DATA_FIM,
        t1.marca AS MARCA,
        t1.negociacao AS  NEGOCIACAO,
        CASE 
            WHEN t1.ultima_chance = 1 THEN 'SIM'
            WHEN t1.ultima_chance = 0 THEN 'NÃO'
            ELSE NULL 
        END AS  ULTIMA_CHANCE,
        t1.motivo_saida,
        t1.data_cancelamento_contrato,
        t1.data_pagamento,
        t1.regional AS REGIONAL,
        t3.cluster AS CLUSTER,
        t1.faturas_abertas AS FATURAS_ABERTAS,
        t1.fatura_motivadora AS FATURA_MOTIVADORA
    FROM {{ get_catalogo('gold') }}.mailing.tbl_analitico t1
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade t3 ON (t2.unidade_atendimento = t3.sigla AND t3.fonte = 'AIR')
    WHERE data_referencia = {{ date_reference }} AND t1.motivo_cancelamento_contrato <> 'INVOLUNTÁRIO'
{% endmacro %}
