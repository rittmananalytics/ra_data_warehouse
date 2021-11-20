{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), numbers AS (

    SELECT *
    FROM {{ ref('utils__facebook_ads__numbers') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_caption,
        object_story_link_data_description,
        object_story_link_data_link,
        object_story_link_data_message,
        object_story_link_data_child_attachments AS child_attachments
    FROM base
    where object_story_link_data_child_attachments is not null

), flattened_child_attachments AS (

    SELECT
        _fivetran_id,
        creative_id,
        object_story_link_data_caption AS caption,
        object_story_link_data_description AS description,
        object_story_link_data_message AS message,
        numbers.generated_number - 1 AS index,
        json_extract_array_element_text(required_fields.child_attachments, numbers.generated_number::int - 1, true) AS element
    FROM required_fields
    inner join numbers
        on json_array_length(required_fields.child_attachments) >= numbers.generated_number

), extracted_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        caption,
        description,
        message,
        index,
        json_extract_path_text(element,'link') AS link,
        json_extract_path_text(element,'url_tags') AS url_tags
    FROM flattened_child_attachments

)

SELECT *
FROM extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
