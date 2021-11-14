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
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message,
        object_story_link_data_child_attachments as child_attachments
    from base
    where object_story_link_data_child_attachments is not null

), flattened_child_attachments as (

    select
        _fivetran_id,
        creative_id,
        object_story_link_data_caption as caption,
        object_story_link_data_description as description,
        object_story_link_data_message as message,
        numbers.generated_number - 1 as index,
        json_extract_array_element_text(required_fields.child_attachments, numbers.generated_number::int - 1, true) as element
    from required_fields
    inner join numbers
        on json_array_length(required_fields.child_attachments) >= numbers.generated_number

), extracted_fields as (

    select
        _fivetran_id,
        creative_id,
        caption,
        description,
        message,
        index,
        json_extract_path_text(element,'link') as link,
        json_extract_path_text(element,'url_tags') as url_tags
    from flattened_child_attachments

)

select *
from extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
