{% if not var("enable_hubspot_email_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_email_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_email_stitch_campaigns_table'),unique_column='id') }}
),
renamed as (
  select * from (
    SELECT
      concat('{{ var('stg_hubspot_email_id-prefix') }}',cast(id as string))              as ad_campaign_id,
      name                                   as ad_campaign_name,
      case when max(_sdc_received_at) over (partition by id) < {{ dbt_utils.current_timestamp() }} then 'PAUSED' else 'ACTIVE' end           as ad_campaign_status,
      type as campaign_buying_type,
      min(_sdc_received_at) over (partition by id)  as ad_campaign_start_date,
      max(_sdc_received_at) over (partition by id)  as ad_campaign_end_date,
      'Hubspot Email' as ad_network
    FROM source
    )
  {{ dbt_utils.group_by(n=7) }}
)
{% endif %}
select * from renamed
