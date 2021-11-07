{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") and var("product_warehouse_event_sources") %}
{{config(alias='contacts_web_event_history_xa')}}

SELECT
  c.contact_pk,
  e.blended_user_id as contact_email,
  e.web_event_pk,
  e.event_type,
  e.event_ts,
  e.event_details,
  e.page_title,
  e.page_url,
  e.ip
FROM
  {{ ref('wh_web_events_fact') }} e
JOIN
  {{ ref('wh_contacts_dim') }} c
ON
  e.blended_user_id IN UNNEST(c.all_contact_emails)
WHERE
  blended_user_id LIKE '%@%'
  {% else %} {{config(enabled=false)}} {% endif %}
