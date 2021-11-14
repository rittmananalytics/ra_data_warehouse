{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

  select *
  from {{ ref('stg_facebook_ads__creative_history') }}

), required_fields as (

  select
    _fivetran_id,
    creative_id,
    template_app_link_spec_ios,
    template_app_link_spec_ipad,
    template_app_link_spec_android,
    template_app_link_spec_iphone
  from base

{% for app in ['ios','ipad','android','iphone'] %}

), unnested_{{ app }} as (

  select
    _fivetran_id,
    creative_id,
    '{{ app }}' as app_type,
    json_extract_scalar(element, '$.index') as index,
    json_extract_scalar(element, '$.app_name') as app_name,
    json_extract_scalar(element, '$.app_store_id') as app_store_id,
    json_extract_scalar(element, '$.class') as class_name,
    json_extract_scalar(element, '$.package') as package_name,
    json_extract_scalar(element, '$.template_page') as template_page
  from required_fields
  left join unnest(json_extract_array(template_app_link_spec_{{ app }})) as element

{% endfor %}

), unioned as (

    select * from unnested_ios
    union all
    select * from unnested_iphone
    union all
    select * from unnested_ipad
    union all
    select * from unnested_android

)

select *
from unioned

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
