{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('stg_facebook_ads__creative_history') }}

), required_fields as (

    select
        _fivetran_id,
        creative_id,
        parse_json(url_tags) as url_tags
    from base
    where url_tags is not null


), flattened_url_tags as (

    select
        _fivetran_id,
        creative_id,
        url_tags.value:key::string as key,
        url_tags.value:value::string as value,
        url_tags.value:type::string as type
    from required_fields,
    lateral flatten( input => url_tags ) as url_tags


)

select *
from flattened_url_tags

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
