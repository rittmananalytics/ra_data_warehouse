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
        creative_id,
        url_tags
    from base
    where url_tags is not null


), flattened_url_tags as (

    select
        _fivetran_id,
        creative_id,
        json_extract_array_element_text(required_fields.url_tags, numbers.generated_number::int - 1, true) as element
    from required_fields
    inner join numbers
        on json_array_length(required_fields.url_tags) >= numbers.generated_number


), extracted_fields as (

    select
        _fivetran_id,
        creative_id,
        json_extract_path_text(element,'key') as key,
        json_extract_path_text(element,'value') as value,
        json_extract_path_text(element,'type') as type
    from flattened_url_tags

)

select *
from extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
