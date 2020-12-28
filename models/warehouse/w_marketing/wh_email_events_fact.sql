{% if not var("enable_mailchimp_email_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='email_events_fact'
    )
}}
{% endif %}

with ad_campaigns_dim as (
      select *
      from {{ ref('wh_ad_campaigns_dim') }}
),
email_lists_dim as (
  select *
  from {{ ref('wh_email_lists_dim') }}
)
,
contacts_dim AS
  (
  SELECT *
  FROM   {{ ref('wh_contacts_dim') }}
),
email_events AS
  (
    SELECT *
    FROM   {{ ref('int_email_events') }}
  )
SELECT

    GENERATE_UUID() as email_event_pk,
    c.contact_pk,
    l.list_pk,
    k.ad_campaign_pk,
    o.* except (list_id,
               contact_id)
FROM
   email_events o
JOIN contacts_dim c
   ON o.contact_id IN UNNEST(c.all_contact_ids)
LEFT JOIN ad_campaigns_dim k
   ON o.ad_campaign_id = k.ad_campaign_id
LEFT JOIN email_lists_dim l
   ON o.list_id = l.list_id
