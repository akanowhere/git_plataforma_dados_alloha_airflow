WITH source AS (
  SELECT * FROM {{ source('fintalk_alloha', 'events') }}
),

transformed AS (
  SELECT DISTINCT
    customer_id,
    channel,
    customer_extras["phone"] AS phone,
    customer_extras["contact"] AS contact,
    message['array_element']['type'] AS type,
    message['array_element']['msg'] AS msg,
    message['array_element']['intent_name'] AS intent_name,
    CAST(message['array_element']['is_fallback'] AS BOOLEAN) AS is_fallback,
    CAST(message['array_element']['event_redirection'] AS BOOLEAN) AS event_redirection,
    CAST(message['array_element']['anon'] AS BOOLEAN) AS anon,
    message['array_element']['subtitle'] AS subtitle,
    message['array_element']['timestamp'] AS timestamp,
    message['array_element']['content_type'] AS content_type,
    message['array_element']['template'] AS template,
    message['array_element']['parameters'] AS parameters,
    message['array_element']['external_id'] AS external_id,
    message['array_element']['error_code'] AS error_code,
    message['array_element']['error_message'] AS error_message,
    incoming_messages,
    outgoing_messages,
    total_intents,
    duration,
    created_at,
    bot,
    is_audio_input

  FROM source
)

SELECT *
FROM transformed
