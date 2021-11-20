{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base AS (

  SELECT *
  FROM {{ ref('stg_facebook_ads__creative_history') }}

), numbers AS (

  SELECT *
  FROM {{ ref('utils__facebook_ads__numbers')}}

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

), flattened_{{ app }} AS (

  SELECT
    _fivetran_id,
    creative_id,
    '{{ app }}'::varchar AS app_type,
    json_extract_array_element_text(required_fields.template_app_link_spec_{{ app }}, numbers.generated_number::int - 1, true) AS element
  FROM required_fields
  inner join numbers
      on json_array_length(required_fields.template_app_link_spec_{{ app }}) >= numbers.generated_number

), extracted_{{ app }} AS (

  SELECT
    _fivetran_id,
    creative_id,
    app_type,
    json_extract_path_text(element,'index') AS index,
    json_extract_path_text(element,'app_name') AS app_name,
    json_extract_path_text(element,'app_store_id') AS app_store_id,
    json_extract_path_text(element,'class') AS class_name,
    json_extract_path_text(element,'package') AS package_name,
    json_extract_path_text(element,'template_page') AS template_page
  FROM flattened_{{ app }}

{% endfor %}

), unioned AS (

    SELECT * FROM extracted_ios
    union all
    SELECT * FROM extracted_iphone
    union all
    SELECT * FROM extracted_ipad
    union all
    SELECT * FROM extracted_android

)

SELECT *
FROM unioned

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
