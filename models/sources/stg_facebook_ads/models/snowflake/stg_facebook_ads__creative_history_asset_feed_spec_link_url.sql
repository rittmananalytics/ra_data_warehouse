{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('stg_facebook_ads__creative_history') }}

), required_fields as (

    select
        _fivetran_id,
        parse_json(asset_feed_spec_link_urls) as asset_feed_spec_link_urls
    from base
    where asset_feed_spec_link_urls is not null

), flattened as (

    select
        _fivetran_id,
        nullif(asset_feed_spec_link_urls.value:display_url::string, '') as display_url,
        nullif(asset_feed_spec_link_urls.value:website_url::string, '') as website_url,
        asset_feed_spec_link_urls.index as index
    from required_fields,
    lateral flatten( input => asset_feed_spec_link_urls ) as asset_feed_spec_link_urls

)

select *
from flattened

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
