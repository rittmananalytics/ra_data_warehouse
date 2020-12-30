{% if var("subscriptions_warehouse_sources")  %}


with plans_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_plans') }}
  )
select * from plans_merge_list

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
