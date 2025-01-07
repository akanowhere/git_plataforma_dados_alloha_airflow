{{
  config(
    materialized='incremental',
    unique_key='sk_chamada'
    )
}}

WITH stg_disparos_whatsapp AS (
  SELECT * FROM {{ ref('vw_infobip_disparos_whatsapp') }}
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['message_id', '_file_generation_date']) }} AS sk_chamada,
    message_id,
    account_name,
    traffic_source,
    communication_name,
    communication_scheduled_for,
    communication_start_date,
    communication_template,
    from,
    to,
    send_at,
    country_prefix,
    country_name,
    network_name,
    purchase_price,
    status,
    reason,
    action,
    error_group,
    error_name,
    done_at,
    text,
    messages_count,
    service_name,
    user_name,
    seen_at,
    clicks,
    paired_message_id,
    data_payload,
    campaign_reference,
    application_id,
    entity_id,
    _file_generation_date

  FROM stg_disparos_whatsapp
)

SELECT *
FROM
  final

{% if is_incremental() %}

  WHERE sk_chamada NOT IN (SELECT sk_chamada FROM {{ this }})

{% endif %}
