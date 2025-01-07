{% macro get_columns(schema_name, table_name, list_exclude_columns) %}
    {# 
        Macro para obter uma lista de colunas de uma tabela, excluindo colunas especificadas.

        Parâmetros:
        - schema_name: Nome do schema onde a tabela está localizada.
        - table_name: Nome da tabela da qual as colunas serão extraídas.
        - list_exclude_columns: Lista de nomes de colunas a serem excluídas da lista de colunas retornadas.

        Retorno:
        - Lista de colunas (string): Uma lista de nomes de colunas separados por vírgula, excluindo as colunas especificadas.

        Exemplo:
        {% set column_list = get_columns('mailing', 'tbl_analitico', ['aging_atual']) %}
    #}


    {%- set relation = adapter.get_relation(
        database=None,
        schema=schema_name,
        identifier=table_name
    ) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}

    {% set columns_to_include = [] %}

    {% for column in columns %}
        {% if column.name not in list_exclude_columns %}
            {% do columns_to_include.append(column.name) %}
        {% endif %}
    {% endfor %}

    {{ return(columns_to_include | join(', ')) }}
{% endmacro %}