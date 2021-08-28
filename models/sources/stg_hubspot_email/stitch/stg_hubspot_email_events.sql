{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}
{% if var("stg_hubspot_email_etl") == 'stitch' %}

with source as (
  select *
  from {{ source('stitch_hubspot_email', 'email_events') }}
),
renamed as (
  SELECT
  cast(null as {{ dbt_utils.type_string() }}) as list_id,
  concat('{{ var('stg_hubspot_email_id-prefix') }}',cast(emailcampaignid as string)) as ad_campaign_id,
  cast(contact_id as string) as contact_id,
  created as event_ts,
  cast(lower(type) as {{ dbt_utils.type_string() }}) as action,
  cast(response as {{ dbt_utils.type_string() }}g) as type,
  cast(recipient as {{ dbt_utils.type_string() }}) as email_address,
  cast(url as {{ dbt_utils.type_string() }}) as url
FROM
  source e
LEFT JOIN
  {{ ref('stg_hubspot_crm_contacts') }} c
on e.recipient = c.contact_email
{{ dbt_utils.group_by(n=8) }}
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
