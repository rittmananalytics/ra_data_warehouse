{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base as (

    select *
    from {{ ref('int__facebook_ads__carousel_media_prep') }}

), numbers as (

    select *
    from {{ ref('utils__facebook_ads__numbers') }}

), unnested as (

    select

        base._fivetran_id,
        base.creative_id,
        base.index,
        json_extract_array_element_text(base.url_tags, numbers.generated_number::int - 1, true) as element
    from base
    inner join numbers
        on json_array_length(base.url_tags) >= numbers.generated_number

), extracted_fields as (

    select
        _fivetran_id,
        creative_id,
        index,
        json_extract_path_text(element,'key') as key,
        json_extract_path_text(element,'value') as value
    from unnested

)

select *
from extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
