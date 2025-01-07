{% macro get_calendar(date_start, date_end) %}
    {# 
        Macro para gerar uma lista de todos os dias entre duas datas.

        Parâmetros:
        - date_start: Data de início do intervalo, no formato adequado para SQL.
        - date_end: Data de fim do intervalo, no formato adequado para SQL.

        Retorno:
        - SQL Query: Uma consulta SQL que gera uma lista de datas entre `date_start` e `date_end`, inclusive.

        Exemplo:
        {% set calendar_query = get_calendar('2024-08-01', '2024-08-10') %}
    #}


WITH date_range AS (
    SELECT explode(sequence(
        to_date('{{ date_start }}'),
        to_date('{{ date_end }}'),
        Interval 1 day
    )) AS DATA
)
SELECT DATA
FROM date_range
ORDER BY DATA

{% endmacro %}