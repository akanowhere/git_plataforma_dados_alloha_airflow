{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
        Retorna o nome do schema padrão ou um schema personalizado após remover espaços em branco.

        Parâmetros:
        - custom_schema_name: Nome do schema desejado (opcional). Se não fornecido, usa o schema padrão.
        - node: Nó de contexto atual do DBT (não utilizado diretamente).

        Retorno:
        - Nome do schema como uma string.
    #}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}