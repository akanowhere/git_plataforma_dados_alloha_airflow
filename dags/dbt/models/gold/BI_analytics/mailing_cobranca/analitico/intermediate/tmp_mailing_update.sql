{{
    config(
        materialized='ephemeral'
    )
}}
{% set dates = get_dates(['first_day_month', 'last_month', 'today'],None, 'mailing_update') %}
SELECT 
   *
FROM {{ get_catalogo('gold') }}.mailing.tbl_analitico 
WHERE
{% if dates.today.day == 1 %}
	data_referencia = '{{ dates.last_month }}'
{% else %}
	data_referencia = '{{ dates.first_day_month }}'
{% endif %}