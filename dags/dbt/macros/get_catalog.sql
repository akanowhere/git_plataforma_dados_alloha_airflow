{% macro get_catalogo(layer) %}
    {# 
        Retornar o nome do catálogo apropriado com base no ambiente de execução
        Se o ambiente de destino (target.name) for 'dev', a macro retorna o nome do catálogo com '_dev' anexado ao final.
        Caso contrário, retorna o nome do catálogo sem alterações.
    #}
    
    {% set result = layer %}
    {% if target.name == 'dev' %}
        {% set result = result ~ '_dev' %}
    {% endif %}
    
    {{ return(result)}}
{% endmacro %}
