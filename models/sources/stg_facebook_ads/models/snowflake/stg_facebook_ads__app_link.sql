{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

  SELECT *
  FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

  SELECT
    _fivetran_id,
    creative_id,
    parse_json(template_app_link_spec_ios) AS template_app_link_spec_ios,
    parse_json(template_app_link_spec_ipad) AS template_app_link_spec_ipad,
    parse_json(template_app_link_spec_android) AS template_app_link_spec_android,
    parse_json(template_app_link_spec_iphone) AS template_app_link_spec_iphone
  FROM base

{% for app in ['ios','ipad','android','iphone'] %}

), flattened_{{ app }} AS (

  SELECT
    _fivetran_id,
    creative_id,
    '{{ app }}' AS app_type,
    element.value:index::string AS index,
    element.value:app_name::string AS app_name,
    element.value:app_store_id::string AS app_store_id,
    element.value:class_name::string AS class_name,
    element.value:package_name::string AS package_name,
    element.value:template_page::string AS template_page
  FROM required_fields,
  lateral flatten( input => template_app_link_spec_{{ app }} ) AS element

{% endfor %}

), unioned AS (

    SELECT * FROM flattened_ios
    union all
    SELECT * FROM flattened_iphone
    union all
    SELECT * FROM flattened_ipad
    union all
    SELECT * FROM flattened_android

)

SELECT *
FROM unioned

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
