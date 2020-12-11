{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source as (
  select * from
  from {{ target.database}}.{{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_campaigns_table') }}

),
renamed as (
    select
      cast(content_id as string)              as ad_campaign_id,
      name                                   as ad_campaign_name,
      cast(null as string)           as ad_campaign_status,
      type as campaign_buying_type,
      cast(null as timestamp)  as ad_campaign_start_date,
      cast(null as timestamp)  as ad_campaign_end_date,
      'Hubspot Email' as ad_network
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stg_hubspot_crm_stitch_schema'),var('stg_hubspot_crm_stitch_campaigns_table'),'contentid') }}

),
renamed as (
  select * from (
    SELECT
      cast(contentid as string)              as ad_campaign_id,
      name                                   as ad_campaign_name,
      case when max(_sdc_received_at) over (partition by contentid) < current_timestamp then 'PAUSED' else 'ACTIVE' end           as ad_campaign_status,
      type as campaign_buying_type,
      min(_sdc_received_at) over (partition by contentid)  as ad_campaign_start_date,
      max(_sdc_received_at) over (partition by contentid)  as ad_campaign_end_date,
      'Hubspot Email' as ad_network
    FROM `ra-development.stitch_hubspot.campaigns`
    WHERE appname = 'Batch')
  group by 1,2,3,4,5,6,7
)
{% endif %}
select * from renamed
