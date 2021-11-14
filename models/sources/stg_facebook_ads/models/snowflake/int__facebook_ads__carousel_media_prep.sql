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
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message,
        parse_json(object_story_link_data_child_attachments) as child_attachments
    from base
    where object_story_link_data_child_attachments is not null

), flattened_child_attachments as (

    select
        _fivetran_id,
        creative_id,
        object_story_link_data_caption as caption,
        object_story_link_data_description as description,
        object_story_link_data_message as message,
        child_attachments as element,
        attachments.index as index,
        attachments.value:link  as link,
        attachments.value:url_tags as url_tags

    from required_fields,
    lateral flatten( input => child_attachments ) as attachments

)

select *
from flattened_child_attachments

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
