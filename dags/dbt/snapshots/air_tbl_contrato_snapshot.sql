{% snapshot air_tbl_contrato_snapshot %}

{{
    config(
      target_database='silver_dev',
      target_schema='snapshots_test',
      unique_key='id',

      strategy='timestamp',
      updated_at='data_alteracao',
    )
}}

with
  latest as (
    select
      *,
      row_number() over (partition by id order by data_alteracao desc) as rn

    from {{ source('air_comercial', 'tbl_contrato') }} version as of 15

    where id = 5765942

    qualify rn = 1

  )

select * 
from latest

-- select * from {{ ref('vw_air_tbl_contrato') }} where id = 5765942

{% endsnapshot %}
