with sde_email_campaign_events_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_mailchimp_email_events') }}
  )
select * from sde_email_campaign_events_merge_list
