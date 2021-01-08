{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("crm_warehouse_contact_sources") and var("marketing_warehouse_deal_sources")%}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") and 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}


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
select *
from   renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
