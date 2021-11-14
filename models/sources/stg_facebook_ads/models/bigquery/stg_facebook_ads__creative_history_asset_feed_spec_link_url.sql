{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('stg_facebook_ads__creative_history') }}

), required_fields as (

    select
        _fivetran_id,
        asset_feed_spec_link_urls
    from base
    where asset_feed_spec_link_urls is not null

), unnested as (

    select
        _fivetran_id,
        nullif(json_extract_scalar(elements,'$.display_url'),'') as display_url,
        nullif(json_extract_scalar(elements,'$.website_url'),'') as website_url,
        row_number() over (partition by _fivetran_id) as index
    from required_fields
    left join unnest(json_extract_array(asset_feed_spec_link_urls)) as elements

)

select *
from unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
