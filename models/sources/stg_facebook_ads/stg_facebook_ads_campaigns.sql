{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_campaign_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_campaign_sources") %}

{% if var("stg_facebook_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(relation=var('stg_facebook_ads_segment_campaigns_table')) }}
),
renamed as (
SELECT
  {{ cast('id','string') }}              as ad_campaign_id,
  name                            as ad_campaign_name,
  effective_status                as ad_campaign_status,
  buying_type                     as campaign_buying_type,
  start_time                      as ad_campaign_start_date,
  stop_time                       as ad_campaign_end_date,
  'Facebook Ads'                  as ad_network
FROM
  source)

{% elif var("stg_facebook_ads_etl") == 'stitch' %}

with source as (

  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_campaigns_table'),unique_column='id') }}

),
renamed as (

    select
    {{ cast('id','string') }}        as ad_campaign_id,
    name                      as ad_campaign_name,
    effective_status          as ad_campaign_status,
    effective_status          as campaign_effective_status,
    start_time                as ad_campaign_start_date,
     {{ cast(datatype='timestamp') }}   as ad_campaign_end_date,
    'Facebook Ads'            as ad_network

    from source

)
{% else %}
    {{ exceptions.raise_compiler_error(var("stg_facebook_ads_etl") ~" not supported in this data source") }}
{% endif %}

select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
