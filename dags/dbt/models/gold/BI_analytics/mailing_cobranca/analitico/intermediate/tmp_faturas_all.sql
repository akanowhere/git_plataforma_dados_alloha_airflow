{{ config(
    materialized='ephemeral'
) }}

{% set dates = get_dates(['today'], None,'tpm_all_faturas') %}

WITH tmp_faturas_all AS (
SELECT 
        *,
		ROW_NUMBER() OVER(PARTITION BY codigo_fatura_sydle ORDER BY data_atualizacao DESC) is_last_corrigido
    FROM gold.sydle.dim_faturas_all_versions
	WHERE data_atualizacao < '{{ dates.today }}'
)
SELECT 
    *
FROM tmp_faturas_all
WHERE
    is_last_corrigido = 1