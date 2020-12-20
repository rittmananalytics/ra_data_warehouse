{% if not var("enable_hubspot_email_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_email_etl") == 'fivetran' %}
with source as (
  select * from
  from {{ target.database}}.{{ var('stg_hubspot_email_fivetran_schema') }}.{{ var('stg_hubspot_email_fivetran_campaigns_table') }}

),
renamed as (
    select
      concat('{{ var('stg_hubspot_email_id-prefix') }}',cast(id as string))              as ad_campaign_id,
      name                                   as ad_campaign_name,
      cast(null as string)           as ad_campaign_status,
      type as campaign_buying_type,
      cast(null as timestamp)  as ad_campaign_start_date,
      cast(null as timestamp)  as ad_campaign_end_date,
      'Hubspot Email' as ad_network
    from source
)
{% elif var("stg_hubspot_email_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stg_hubspot_email_stitch_schema'),var('stg_hubspot_email_stitch_campaigns_table'),'id') }}

),
renamed as (
  select * from (
    SELECT
      concat('{{ var('stg_hubspot_email_id-prefix') }}',cast(id as string))              as ad_campaign_id,
      name                                   as ad_campaign_name,
      case when max(_sdc_received_at) over (partition by id) < current_timestamp then 'PAUSED' else 'ACTIVE' end           as ad_campaign_status,
      type as campaign_buying_type,
      min(_sdc_received_at) over (partition by id)  as ad_campaign_start_date,
      max(_sdc_received_at) over (partition by id)  as ad_campaign_end_date,
      'Hubspot Email' as ad_network
    FROM source
    WHERE appname = 'Batch')
  group by 1,2,3,4,5,6,7
)
{% endif %}
select * from renamed
