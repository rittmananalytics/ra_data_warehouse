{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        parse_json(asset_feed_spec_link_urls) AS asset_feed_spec_link_urls
    FROM base
    where asset_feed_spec_link_urls is not null

), flattened AS (

    SELECT
        _fivetran_id,
        nullif(asset_feed_spec_link_urls.value:display_url::string, '') AS display_url,
        nullif(asset_feed_spec_link_urls.value:website_url::string, '') AS website_url,
        asset_feed_spec_link_urls.index AS index
    FROM required_fields,
    lateral flatten( input => asset_feed_spec_link_urls ) AS asset_feed_spec_link_urls

)

SELECT *
FROM flattened

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
