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
        asset_feed_spec_link_urls
    FROM base
    where asset_feed_spec_link_urls is not null

), flattened AS (

    SELECT
        _fivetran_id,
        json_extract_array_element_text(required_fields.asset_feed_spec_link_urls, numbers.generated_number::int - 1, true) AS element,
        numbers.generated_number - 1 AS index
    FROM required_fields
    inner join numbers
        on json_array_length(required_fields.asset_feed_spec_link_urls) >= numbers.generated_number

), extracted_fields AS (

    SELECT
        _fivetran_id,
        index,
        json_extract_path_text(element,'display_url') AS display_url,
        json_extract_path_text(element,'website_url') AS website_url
    FROM flattened

)

SELECT *
FROM extracted_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
