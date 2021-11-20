{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}
{% if var("stg_hubspot_email_etl") == 'stitch' %}

with source AS (
  SELECT *
  FROM {{ source('stitch_hubspot_email', 'email_events') }}
),
renamed AS (
  SELECT
  CAST(null AS {{ dbt_utils.type_string() }}) AS list_id,
  CONCAT('{{ var('stg_hubspot_email_id-prefix') }}',CAST(emailcampaignid AS string)) AS ad_campaign_id,
  CAST(contact_id AS string) AS contact_id,
  created AS event_ts,
  CAST(lower(type) AS {{ dbt_utils.type_string() }}) AS action,
  CAST(response AS {{ dbt_utils.type_string() }}g) AS type,
  CAST(recipient AS {{ dbt_utils.type_string() }}) AS email_address,
  CAST(url AS {{ dbt_utils.type_string() }}) AS url
FROM
  source e
LEFT JOIN
  {{ ref('stg_hubspot_crm_contacts') }} c
on e.recipient = c.contact_email
{{ dbt_utils.group_by(n=8) }}
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
