{% macro convert_time_to_seconds(column_name) %}
    {#
        Converte uma string de tempo no formato 'HH:MM:SS' para o total de segundos.
        Essa macro lida com horas superiores a 24 e verifica se a string está no formato correto.

        Parâmetros:
        - column_name (string, obrigatório): A string de tempo a ser convertida, no formato 'HH:MM:SS'.

        Retorno:
        - integer: O número total de segundos representado pela string de tempo fornecida.

        Exemplo:
        - Para converter '24:01:00' em segundos:
            {{ convert_time_to_seconds('24:01:00') }}
            -- Retorna: 86460 segundos (24*3600 + 1*60 + 0)
    #}

    {%- set sql_expression = (
        'CASE WHEN ' ~ column_name ~ ' LIKE \'%:%:%\' THEN ' ~
        'CAST(SUBSTRING_INDEX(' ~ column_name ~ ', \':\', 1) AS INTEGER) * 3600 + ' ~
        'CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(' ~ column_name ~ ', \':\', 2), \':\', -1) AS INTEGER) * 60 + ' ~
        'CAST(SUBSTRING_INDEX(' ~ column_name ~ ', \':\', -1) AS INTEGER) ' ~
        'ELSE 0 END'
    ) %}
    {{ sql_expression }}
{% endmacro %}