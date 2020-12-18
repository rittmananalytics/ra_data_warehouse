{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  select *
  from {{ target.database}}.{{ var('stg_hubspot_crm_stitch_schema') }}.{{ var('stg_hubspot_crm_stitch_email_events_table') }}
),
renamed as (
  SELECT
  cast(emailcampaigngroupid as string) as list_id,
  cast(id as string) as send_id,
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
where type not in ('STATUSCHANGE','DELIVERED','PROCESSED')
)
{% endif %}
select * from renamed
