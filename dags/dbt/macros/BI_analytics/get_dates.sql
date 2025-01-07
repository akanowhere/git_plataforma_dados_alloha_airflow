{% macro get_dates(date_keys, set_today=None, script=None) %}
    {# 
        Gera um dicionário de datas ajustadas com base na data atual ou em uma data fornecida.

        Parâmetros:
        - date_keys (list): Uma lista de strings que especifica as datas desejadas no retorno.
            - Opções disponíveis: 'today', 'yesterday', 'first_day_month', 'last_day_month','last_month', 'next_month'.
        - set_today (string, opcional): Uma string no formato 'YYYY-MM-DD' para definir uma data específica como 'today'.
        Se não for fornecida, usa a data e hora atuais.

        Retorno:
        - dict: Um dicionário contendo as datas solicitadas, ajustadas para a zona de tempo (-3 horas/Brasil).

        Exemplos:
        {% set dates = get_dates(['today', 'first_day_month']) %}
        {% set dates = get_dates(['yesterday', 'last_day_month'], '2024-08-14') %}
    #}

    {% set today = modules.datetime.datetime.now() %}
    {% set adjusted_today = today - modules.datetime.timedelta(hours=3) %}

    {% if set_today %}  
        {% set today_date = modules.datetime.datetime.strptime(set_today, '%Y-%m-%d') %}
        {% set today_time = modules.datetime.datetime.now().time() %}
        {% set today = modules.datetime.datetime.combine(today_date, today_time) %}
        {% set adjusted_today = today %}
    {% endif %}
    {% set adjusted_today = adjusted_today.date() %}
    {% set adjusted_yesterday = adjusted_today - modules.datetime.timedelta(days=1) %}
    {% set adjusted_first_day = adjusted_today.replace(day=1) %}
    {% if today.month <= 11 %}
        {% set next_month = adjusted_first_day.replace(month=adjusted_first_day.month + 1) %}
    {% else %}
        {% set next_month = adjusted_first_day.replace(month=1, year=adjusted_first_day.year + 1) %}
    {% endif %}
    {% if today.month > 1 %}
        {% set last_month = adjusted_first_day.replace(month=adjusted_first_day.month - 1) %}
    {% else %}
        {% set last_month = adjusted_first_day.replace(month=12, year=adjusted_first_day.year - 1) %}
    {% endif %}
    {% set adjusted_last_day = next_month.replace(day=1) - modules.datetime.timedelta(days=1) %}
    
    {% set dates = {
        'today': adjusted_today,
        'yesterday': adjusted_yesterday,
        'first_day_month': adjusted_first_day,
        'last_day_month': adjusted_last_day,
        'last_month': last_month,
        'next_month': next_month,
    } %}
    
    {% set result = {} %}
    
    {% for key in date_keys %}
        {% set _ = result.update({key: dates[key]}) %}
    {% endfor %}
    
    {{ log(result ~ ' do script: ' ~ script, info=True) }}
    {{ return(result)}} 

{% endmacro %}
