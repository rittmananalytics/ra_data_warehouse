{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message,
        parse_json(object_story_link_data_child_attachments) AS child_attachments
    FROM base
    where object_story_link_data_child_attachments is not null

), flattened_child_attachments AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_caption AS caption,
        object_story_link_data_description AS description,
        object_story_link_data_message AS message,
        child_attachments AS element,
        attachments.index AS index,
        attachments.value:link  AS link,
        attachments.value:url_tags AS url_tags

    FROM required_fields,
    lateral flatten( input => child_attachments ) AS attachments

)

SELECT *
FROM flattened_child_attachments

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
