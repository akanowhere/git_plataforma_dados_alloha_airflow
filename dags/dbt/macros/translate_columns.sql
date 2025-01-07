{% macro translate_column(column_name) %}
    {# 
        Remove caracteres especiais e letras acentuadas de uma coluna, substituindo-os por caracteres não acentuados.

        Parâmetros:
        - column_name: Nome da coluna da qual os caracteres especiais e acentuados serão removidos.

        Retorno:
        - SQL Expression: A expressão SQL que aplica a função `TRANSLATE` para substituir caracteres especiais e acentuados.

        Exemplo:
        {% set cleaned_column = translate_column('column_name') %}
    #}

    TRANSLATE(
        {{ column_name }},
        "ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ",
        "AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao"
    )
{% endmacro %}
