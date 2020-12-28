{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deals_table'),unique_column='dealid') }}
),
renamed as (
SELECT
  dealid as deal_id,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',associatedvids.value) as contact_id,
FROM
  source,
  unnest(associations.associatedvids) as associatedvids
)
{% endif %}
select *
from   renamed
