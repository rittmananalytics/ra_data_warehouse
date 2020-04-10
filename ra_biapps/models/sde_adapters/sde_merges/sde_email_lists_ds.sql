with sde_email_lists_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_mailchimp_email_lists') }}
  )
select * from sde_email_lists_merge_list
