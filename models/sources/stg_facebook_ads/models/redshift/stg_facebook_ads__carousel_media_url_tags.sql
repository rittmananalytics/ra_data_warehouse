{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base AS (

    SELECT *
    FROM {{ ref('int__facebook_ads__carousel_media_prep') }}

), numbers AS (

    SELECT *
    FROM {{ ref('utils__facebook_ads__numbers') }}

), unnested AS (

    SELECT

        base._fivetran_id,
        base.creative_id,
        base.index,
        json_extract_array_element_text(base.url_tags, numbers.generated_number::int - 1, true) AS element
    FROM base
    inner join numbers
        on json_array_length(base.url_tags) >= numbers.generated_number

), extracted_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        index,
        json_extract_path_text(element,'key') AS key,
        json_extract_path_text(element,'value') AS value
    FROM unnested

)

SELECT *
FROM extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
