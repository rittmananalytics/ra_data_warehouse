{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base as (

    select *
    from {{ ref('stg_facebook_ads__creative_history') }}

), required_fields as (

    select
        _fivetran_id,
        creative_id,
        url_tags
    from base
    where url_tags is not null

), cleaned_json as (

    select
        _fivetran_id,
        creative_id,
        json_extract_array(replace(trim(url_tags, '"'),'\\','')) as cleaned_url_tags
    from required_fields

), unnested as (

    select _fivetran_id, creative_id, url_tag_element
    from cleaned_json
    left join unnest(cleaned_url_tags) as url_tag_element
    where cleaned_url_tags is not null

), fields as (

    select
        _fivetran_id,
        creative_id,
        json_extract_scalar(url_tag_element, '$.key') as key,
        json_extract_scalar(url_tag_element, '$.value') as value,
        json_extract_scalar(url_tag_element, '$.type') as type
    from unnested

)

select *
from fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
