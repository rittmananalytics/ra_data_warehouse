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
        url_tags
    FROM base
    where url_tags is not null

), cleaned_json AS (

    SELECT
        _fivetran_id,
        creative_id,
        json_extract_array(replace(trim(url_tags, '"'),'\\','')) AS cleaned_url_tags
    FROM required_fields

), unnested AS (

    SELECT _fivetran_id, creative_id, url_tag_element
    FROM cleaned_json
    left join unnest(cleaned_url_tags) AS url_tag_element
    where cleaned_url_tags is not null

), fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        json_extract_scalar(url_tag_element, '$.key') AS key,
        json_extract_scalar(url_tag_element, '$.value') AS value,
        json_extract_scalar(url_tag_element, '$.type') AS type
    FROM unnested

)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
