{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base as (

    select *
    from {{ ref('stg_facebook_ads__creative_history') }}

), numbers as (

    select *
    from {{ ref('utils__facebook_ads__numbers') }}

), required_fields as (

    select
        _fivetran_id,
        asset_feed_spec_link_urls
    from base
    where asset_feed_spec_link_urls is not null

), flattened as (

    select
        _fivetran_id,
        json_extract_array_element_text(required_fields.asset_feed_spec_link_urls, numbers.generated_number::int - 1, true) as element,
        numbers.generated_number - 1 as index
    from required_fields
    inner join numbers
        on json_array_length(required_fields.asset_feed_spec_link_urls) >= numbers.generated_number

), extracted_fields as (

    select
        _fivetran_id,
        index,
        json_extract_path_text(element,'display_url') as display_url,
        json_extract_path_text(element,'website_url') as website_url
    from flattened

)

select *
from extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
