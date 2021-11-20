{% if target.type == 'bigquery'  %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_child_attachments,
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message
    FROM base
    where object_story_link_data_child_attachments is not null

), unnested AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_caption AS caption,
        object_story_link_data_description AS description,
        object_story_link_data_message AS message,
        json_extract_scalar(element, '$.link') AS link,
        json_extract_array(element, '$.url_tags') AS url_tags,
        row_number() over (PARTITION BY_fivetran_id, creative_id) AS index
    FROM required_fields
    left join unnest(json_extract_array(object_story_link_data_child_attachments)) AS element

)

SELECT *
FROM unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
