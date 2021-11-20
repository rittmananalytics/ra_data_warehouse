{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

  SELECT *
  FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

  SELECT
    _fivetran_id,
    creative_id,
    template_app_link_spec_ios,
    template_app_link_spec_ipad,
    template_app_link_spec_android,
    template_app_link_spec_iphone
  FROM base

{% for app in ['ios','ipad','android','iphone'] %}

), unnested_{{ app }} AS (

  SELECT
    _fivetran_id,
    creative_id,
    '{{ app }}' AS app_type,
    json_extract_scalar(element, '$.index') AS index,
    json_extract_scalar(element, '$.app_name') AS app_name,
    json_extract_scalar(element, '$.app_store_id') AS app_store_id,
    json_extract_scalar(element, '$.class') AS class_name,
    json_extract_scalar(element, '$.package') AS package_name,
    json_extract_scalar(element, '$.template_page') AS template_page
  FROM required_fields
  left join unnest(json_extract_array(template_app_link_spec_{{ app }})) AS element

{% endfor %}

), unioned AS (

    SELECT * FROM unnested_ios
    union all
    SELECT * FROM unnested_iphone
    union all
    SELECT * FROM unnested_ipad
    union all
    SELECT * FROM unnested_android

)

SELECT *
FROM unioned

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
