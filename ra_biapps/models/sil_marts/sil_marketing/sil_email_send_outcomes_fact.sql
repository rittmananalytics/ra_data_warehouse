{% if not var("enable_mailchimp_email_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='email_send_outcomes_fact'
    )
}}
{% endif %}

with email_sends_dim as (
      select *
      from {{ ref('sil_email_sends_dim') }}
),
email_lists_dim as (
      select *
      from {{ ref('sil_email_lists_dim') }}
),
contacts_dim AS
  (
  SELECT *
  FROM   {{ ref('sil_contacts_dim') }}
),
email_send_outcomes_fs AS
  (
    SELECT *
    FROM   {{ ref('t_email_send_outcomes_fs') }}
  )
SELECT

    GENERATE_UUID() as send_outcome_pk,
    c.contact_pk,
    l.list_pk,
    s.send_pk,
    o.* except (list_id,
               send_id,
               contact_id)
FROM
   email_send_outcomes_fs o
JOIN contacts_dim c
   ON concat('mailchimp-',o.contact_id) IN UNNEST(c.all_contact_ids)
JOIN email_lists_dim l
   ON o.list_id = l.list_id
JOIN email_sends_dim s
   ON o.send_id = s.send_id
