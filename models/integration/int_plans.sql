{% if not var("enable_stripe_subscriptions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


with plans_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_plans') }}
  )
select * from plans_merge_list
