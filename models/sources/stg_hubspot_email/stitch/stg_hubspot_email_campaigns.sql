{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}
{% if var("stg_hubspot_email_etl") == 'stitch' %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_email','campaigns'),unique_column='id') }}
),
renamed AS (
  SELECT * FROM (
    SELECT
      CONCAT('{{ var('stg_hubspot_email_id-prefix') }}',CAST(id AS {{ dbt_utils.type_string() }}))              AS ad_campaign_id,
      name                                   AS ad_campaign_name,
      case when max(_sdc_received_at) over (PARTITION BYid) < {{ dbt_utils.current_timestamp() }} then 'PAUSED' else 'ACTIVE' end           AS ad_campaign_status,
      type AS campaign_buying_type,
      min(CAST(_sdc_received_at AS {{ dbt_utils.type_timestamp() }} ) ) over (PARTITION BYid)  AS ad_campaign_start_date,
      max(CAST(_sdc_received_at AS {{ dbt_utils.type_timestamp() }} ) ) over (PARTITION BYid)  AS ad_campaign_end_date,
      'Hubspot Email' AS ad_network
    FROM source
    )
  {{ dbt_utils.group_by(n=7) }}
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
