{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("crm_warehouse_contact_sources") and var("marketing_warehouse_deal_sources")%}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") and 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}


with source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','deals'),unique_column='dealid') }}
),
renamed AS (
SELECT
  dealid AS deal_id,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',associatedvids.value) AS contact_id,
FROM
  source,
  unnest(associations.associatedvids) AS associatedvids
)
SELECT *
FROM   renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
