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
        url_tags
    FROM base
    where url_tags is not null


), flattened_url_tags AS (

    SELECT
        _fivetran_id,
        creative_id,
        json_extract_array_element_text(required_fields.url_tags, numbers.generated_number::int - 1, true) AS element
    FROM required_fields
    inner join numbers
        on json_array_length(required_fields.url_tags) >= numbers.generated_number


), extracted_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        json_extract_path_text(element,'key') AS key,
        json_extract_path_text(element,'value') AS value,
        json_extract_path_text(element,'type') AS type
    FROM flattened_url_tags

)

SELECT *
FROM extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
