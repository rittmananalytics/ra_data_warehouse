{% if not var("enable_hubspot_email_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_email_etl") == 'stitch' %}
with source as (
  select *
  from {{ target.database}}.{{ var('stg_hubspot_email_stitch_schema') }}.{{ var('stg_hubspot_email_stitch_email_events_table') }}
),
renamed as (
  SELECT
  cast(null as string) as send_id,
  concat('{{ var('stg_hubspot_email_id-prefix') }}',cast(emailcampaignid as string)) as ad_campaign_id,
  cast(contact_id as string) as contact_id,
  created as event_ts,
  cast(lower(type) as string) as action,
  cast(response as string) as type,
  cast(recipient as string) as email_address,
  cast(url as string) as url
FROM
  source e
LEFT JOIN
  {{ ref('stg_hubspot_crm_contacts') }} c
on e.recipient = c.contact_email
{{ dbt_utils.group_by(n=8) }}
)
{% endif %}
select * from renamed
