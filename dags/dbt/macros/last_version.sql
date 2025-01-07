
{% macro dynamic_table_query(catalog_schema_table, partition_column, order_column) %}
  {# 
    Retorna a versão mais recente da tabela com base em uma coluna de partição e uma coluna de ordenação.

    Parâmetros:
    - catalog_schema_table: Nome completo da tabela no formato `catalog.schema.table`.
    - partition_column: Coluna pela qual os dados serão particionados.
    - order_column: Coluna usada para determinar a versão mais recente, ordenada de forma decrescente.

    Retorno:
    - SQL Query: A query que retorna apenas a linha mais recente para cada partição.

    Exemplo:
    {% set query = dynamic_table_query('catalog.schema.table', 'id', 'updated_at') %}
  #}

SELECT *
FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY {{ partition_column }} ORDER BY {{ order_column }} DESC) AS rn
    FROM
      {{ catalog_schema_table }}
) AS sub
WHERE
  rn = 1

{% endmacro %}
