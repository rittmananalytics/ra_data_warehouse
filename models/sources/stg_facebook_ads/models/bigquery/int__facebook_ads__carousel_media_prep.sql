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
        object_story_link_data_child_attachments,
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message
    from base
    where object_story_link_data_child_attachments is not null

), unnested as (

    select
        _fivetran_id,
        creative_id,
        object_story_link_data_caption as caption,
        object_story_link_data_description as description,
        object_story_link_data_message as message,
        json_extract_scalar(element, '$.link') as link,
        json_extract_array(element, '$.url_tags') as url_tags,
        row_number() over (partition by _fivetran_id, creative_id) as index
    from required_fields
    left join unnest(json_extract_array(object_story_link_data_child_attachments)) as element

)

select *
from unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
