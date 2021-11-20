{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        asset_feed_spec_link_urls
    FROM base
    where asset_feed_spec_link_urls is not null

), unnested AS (

    SELECT
        _fivetran_id,
        nullif(json_extract_scalar(elements,'$.display_url'),'') AS display_url,
        nullif(json_extract_scalar(elements,'$.website_url'),'') AS website_url,
        row_number() over (PARTITION BY_fivetran_id) AS index
    FROM required_fields
    left join unnest(json_extract_array(asset_feed_spec_link_urls)) AS elements

)

SELECT *
FROM unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
