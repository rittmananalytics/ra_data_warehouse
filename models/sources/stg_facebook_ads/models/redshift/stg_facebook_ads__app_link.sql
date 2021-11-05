{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base as (

  select *
  from {{ ref('stg_facebook_ads__creative_history') }}

), numbers as (

  select *
  from {{ ref('utils__facebook_ads__numbers')}}

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

), flattened_{{ app }} as (

  select
    _fivetran_id,
    creative_id,
    '{{ app }}'::varchar as app_type,
    json_extract_array_element_text(required_fields.template_app_link_spec_{{ app }}, numbers.generated_number::int - 1, true) as element
  from required_fields
  inner join numbers
      on json_array_length(required_fields.template_app_link_spec_{{ app }}) >= numbers.generated_number

), extracted_{{ app }} as (

  select
    _fivetran_id,
    creative_id,
    app_type,
    json_extract_path_text(element,'index') as index,
    json_extract_path_text(element,'app_name') as app_name,
    json_extract_path_text(element,'app_store_id') as app_store_id,
    json_extract_path_text(element,'class') as class_name,
    json_extract_path_text(element,'package') as package_name,
    json_extract_path_text(element,'template_page') as template_page
  from flattened_{{ app }}

{% endfor %}

), unioned as (

    select * from extracted_ios
    union all
    select * from extracted_iphone
    union all
    select * from extracted_ipad
    union all
    select * from extracted_android

)

select *
from unioned

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
