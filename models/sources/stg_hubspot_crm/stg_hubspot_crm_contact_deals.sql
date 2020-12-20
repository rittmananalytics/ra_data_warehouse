{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stg_hubspot_crm_stitch_schema'),var('stg_hubspot_crm_stitch_deals_table'),'dealid') }}
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
